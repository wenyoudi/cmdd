"""Writes-Follow-Reads (WFR) consistency experiment for Cassandra.

Each trial initializes parent=0 and child=0 at CL=ALL, then executes:

    B: WRITE parent=1 at QUORUM through node1
    A: READ parent at the tested read CL through node1
    A: if and only if the read returned 1, WRITE child=1 at the tested
       write CL (through node2 normally, or isolated node3 in a partition)

After a successful dependent write, reachable scenario-specific nodes are
queried independently.  A possible WFR anomaly is recorded only when the
same verification query observes parent=0 and child=1.
"""

import argparse
import csv
import json
import re
import subprocess
import time
import uuid

from collections import Counter
from datetime import datetime
from pathlib import Path

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import FallthroughRetryPolicy, WhiteListRoundRobinPolicy


ROOT = Path(__file__).resolve().parent

# (dependency read CL, dependent write CL)
PAIRS = [
    ("ONE", "ONE"),
    ("ONE", "QUORUM"),
    ("QUORUM", "ONE"),
    ("QUORUM", "QUORUM"),
    ("ALL", "ONE"),
    ("ONE", "ALL"),
]

CHAIN = "CMDD_WFR"

# Team-wide 16-column trial schema. Internal bookkeeping fields begin with _
# and are deliberately excluded when trials.csv is written.
TRIAL_FIELDS = [
    "trial_id",
    "model",
    "scenario",
    "client",
    "operation",
    "key",
    "version_written",
    "version_observed",
    "read_cl",
    "write_cl",
    "target_node",
    "success",
    "violation",
    "latency_ms",
    "timestamp",
    "error",
]

SUMMARY_FIELDS = [
    "read_cl",
    "write_cl",
    "trials",
    "dependency_established",
    "completed_trials",
    "successful_verifications",
    "wfr_anomalies",
    "parent_write_failed",
    "dependency_read_failed",
    "dependency_not_established",
    "dependent_write_failed",
    "verify_failed",
]


def connect(port, level):
    """Connect to exactly one published Cassandra node port."""
    profile = ExecutionProfile(
        load_balancing_policy=WhiteListRoundRobinPolicy(["127.0.0.1"]),
        retry_policy=FallthroughRetryPolicy(),
        consistency_level=level,
        request_timeout=15,
    )
    cluster = Cluster(
        ["127.0.0.1"],
        port=port,
        protocol_version=4,
        execution_profiles={EXEC_PROFILE_DEFAULT: profile},
    )
    try:
        return cluster, cluster.connect()
    except Exception:
        cluster.shutdown()
        raise


def docker(*args, check=True):
    result = subprocess.run(
        ["docker", *args],
        capture_output=True,
        text=True,
        timeout=60,
    )
    if check and result.returncode:
        raise RuntimeError(result.stderr or result.stdout)
    return result


def firewall(*args, check=True):
    return docker(
        "exec",
        "--user",
        "root",
        "cassandra-3",
        "iptables",
        "-w",
        "5",
        *args,
        check=check,
    )


def remove_partition():
    for hook in ("INPUT", "OUTPUT"):
        while firewall("-C", hook, "-j", CHAIN, check=False).returncode == 0:
            firewall("-D", hook, "-j", CHAIN)

    if firewall("-S", CHAIN, check=False).returncode == 0:
        firewall("-F", CHAIN)
        firewall("-X", CHAIN)

    # Retain the verified MW recovery check: this also proves iptables works.
    firewall("-S")


def add_partition():
    firewall("-N", CHAIN)
    for direction in ("--dports", "--sports"):
        firewall(
            "-A", CHAIN,
            "-p", "tcp",
            "-m", "multiport",
            direction, "7000,7001",
            "-j", "DROP",
        )
    for hook in ("INPUT", "OUTPUT"):
        firewall("-I", hook, "1", "-j", CHAIN)


def ring_view(node):
    result = docker("exec", f"cassandra-{node}", "nodetool", "status")
    states = re.findall(r"^([UD][NLJM])\s", result.stdout, re.M)
    return Counter(states), result.stdout


def check_ring():
    for node in (1, 2, 3):
        states, output = ring_view(node)
        if states != Counter({"UN": 3}):
            raise RuntimeError(f"node{node}: expected three UN nodes\n{output}")


def wait_topology(scenario, evidence):
    deadline = time.monotonic() + 180
    while True:
        nodes = (1, 2, 3) if scenario == "partition" else (1, 2)
        views = {node: ring_view(node) for node in nodes}

        good = all(
            views[node][0] == Counter({"UN": 2, "DN": 1})
            for node in (1, 2)
        )
        if scenario == "partition":
            good = good and views[3][0] == Counter({"UN": 1, "DN": 2})

        if good:
            evidence["fault_views"] = {
                str(node): view[1] for node, view in views.items()
            }
            return
        if time.monotonic() > deadline:
            raise RuntimeError("Fault topology not confirmed")
        time.sleep(3)


def wait_recovery():
    deadline = time.monotonic() + 300
    while True:
        try:
            check_ring()
            for port in (10001, 10002, 10003):
                cluster, session = connect(port, ConsistencyLevel.ONE)
                try:
                    session.execute("SELECT release_version FROM system.local")
                finally:
                    cluster.shutdown()
            return
        except Exception:
            if time.monotonic() > deadline:
                raise RuntimeError("Recovery not confirmed")
            time.sleep(3)


def make_row(
    context,
    operation,
    target_node,
    version_written="",
    version_observed="",
    read_cl="",
    write_cl="",
):
    return dict(
        context,
        operation=operation,
        target_node=target_node,
        version_written=version_written,
        version_observed=version_observed,
        read_cl=read_cl,
        write_cl=write_cl,
        success=False,
        violation="",
        latency_ms=0,
        timestamp=datetime.now().astimezone().isoformat(),
        error="",
    )


def execute_write(
    session,
    prepared,
    key,
    value,
    write_timestamp,
    consistency,
    context,
    client,
    operation,
    target_node,
):
    row_context = dict(context, client=client)
    row = make_row(
        row_context,
        operation=operation,
        target_node=target_node,
        version_written=value,
        write_cl=consistency,
    )
    started = time.perf_counter()
    try:
        # UPDATE statements place the TIMESTAMP marker before the value marker.
        statement = prepared.bind((write_timestamp, value, key))
        statement.consistency_level = getattr(ConsistencyLevel, consistency)
        session.execute(statement)
        row["success"] = True
    except Exception as exc:
        row["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        row["latency_ms"] = round((time.perf_counter() - started) * 1000, 3)
    return row


def execute_dependency_read(session, prepared, key, consistency, context):
    row = make_row(
        dict(context, client="A"),
        operation="DEPENDENCY_READ",
        target_node="node1",
        read_cl=consistency,
    )
    started = time.perf_counter()
    try:
        statement = prepared.bind((key,))
        statement.consistency_level = getattr(ConsistencyLevel, consistency)
        record = session.execute(statement).one()
        observed = record.parent if record else None
        row["version_observed"] = observed if observed is not None else ""
        row["success"] = True
        row["_dependency_established"] = observed == 1
    except Exception as exc:
        row["error"] = f"{type(exc).__name__}: {exc}"
        row["_dependency_established"] = False
    finally:
        row["latency_ms"] = round((time.perf_counter() - started) * 1000, 3)
    return row


def execute_verification(session, prepared, key, context, target_node):
    """Read parent and child together and return two compatible CSV rows."""
    parent_row = make_row(
        dict(context, client="observer"),
        operation="VERIFY_PARENT",
        target_node=target_node,
        read_cl="ONE",
    )
    child_row = make_row(
        dict(context, client="observer"),
        operation="VERIFY_CHILD",
        target_node=target_node,
        read_cl="ONE",
    )
    started = time.perf_counter()
    try:
        statement = prepared.bind((key,))
        statement.consistency_level = ConsistencyLevel.ONE
        record = session.execute(statement).one()
        parent = record.parent if record else None
        child = record.child if record else None
        anomaly = parent == 0 and child == 1

        parent_row["version_observed"] = parent if parent is not None else ""
        child_row["version_observed"] = child if child is not None else ""
        parent_row["success"] = True
        child_row["success"] = True
        # Mark one row only so summing violation=True counts anomalous node
        # observations, not twice the number of anomalies.
        parent_row["violation"] = False
        child_row["violation"] = anomaly
        parent_row["_verification_success"] = True
        child_row["_verification_success"] = True
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        parent_row["error"] = error
        child_row["error"] = error
        parent_row["_verification_success"] = False
        child_row["_verification_success"] = False
    finally:
        latency = round((time.perf_counter() - started) * 1000, 3)
        parent_row["latency_ms"] = latency
        child_row["latency_ms"] = latency
    return parent_row, child_row


def summarize(rows):
    output = []
    pair_names = []
    for row in rows:
        pair_name = row.get("_pair")
        if pair_name and pair_name not in pair_names:
            pair_names.append(pair_name)

    for pair_name in pair_names:
        read_cl, write_cl = pair_name.split(":")
        group = [row for row in rows if row.get("_pair") == pair_name]
        trial_ids = sorted({row["trial_id"] for row in group})
        counts = {
            "dependency_established": 0,
            "completed_trials": 0,
            "successful_verifications": 0,
            "wfr_anomalies": 0,
            "parent_write_failed": 0,
            "dependency_read_failed": 0,
            "dependency_not_established": 0,
            "dependent_write_failed": 0,
            "verify_failed": 0,
        }

        for trial_id in trial_ids:
            trial = [row for row in group if row["trial_id"] == trial_id]
            parent_writes = [r for r in trial if r["operation"] == "PARENT_WRITE"]
            dependency_reads = [r for r in trial if r["operation"] == "DEPENDENCY_READ"]
            dependent_writes = [r for r in trial if r["operation"] == "DEPENDENT_WRITE"]
            verify_children = [r for r in trial if r["operation"] == "VERIFY_CHILD"]

            if parent_writes and not parent_writes[0]["success"]:
                counts["parent_write_failed"] += 1

            dependency_established = False
            if dependency_reads:
                dependency_read = dependency_reads[0]
                if not dependency_read["success"]:
                    counts["dependency_read_failed"] += 1
                elif dependency_read["version_observed"] != 1:
                    counts["dependency_not_established"] += 1
                else:
                    dependency_established = True
                    counts["dependency_established"] += 1

            dependent_write_succeeded = False
            if dependent_writes:
                if dependent_writes[0]["success"]:
                    dependent_write_succeeded = True
                else:
                    counts["dependent_write_failed"] += 1

            if dependency_established and dependent_write_succeeded:
                counts["completed_trials"] += 1

            for verify in verify_children:
                if verify["success"]:
                    counts["successful_verifications"] += 1
                    if verify["violation"] is True:
                        counts["wfr_anomalies"] += 1
                else:
                    counts["verify_failed"] += 1

        output.append({
            "read_cl": read_cl,
            "write_cl": write_cl,
            "trials": len(trial_ids),
            **counts,
        })
    return output


def save(folder, rows, metadata):
    clean_rows = [
        {field: row.get(field, "") for field in TRIAL_FIELDS}
        for row in rows
    ]
    with (folder / "trials.csv").open(
        "w", newline="", encoding="utf-8-sig"
    ) as file:
        writer = csv.DictWriter(file, fieldnames=TRIAL_FIELDS)
        writer.writeheader()
        writer.writerows(clean_rows)

    with (folder / "summary.csv").open(
        "w", newline="", encoding="utf-8-sig"
    ) as file:
        writer = csv.DictWriter(file, fieldnames=SUMMARY_FIELDS)
        writer.writeheader()
        writer.writerows(summarize(rows))

    (folder / "metadata.json").write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def run(args):
    run_id = str(uuid.uuid4())
    results_root = ROOT.parent.parent / "results" / "writes_follow_reads"
    folder = results_root / f"{args.scenario}_{run_id}"
    folder.mkdir(parents=True, exist_ok=True)

    rows = []
    clusters = []
    sessions = []
    fault_attempted = False

    verification_nodes = (
        ["node1", "node2"]
        if args.scenario == "node_failure"
        else ["node1", "node2", "node3"]
    )
    dependent_write_node = "node3" if args.scenario == "partition" else "node2"
    dependent_write_index = 2 if args.scenario == "partition" else 1

    metadata = {
        "run_id": run_id,
        "model": "WFR",
        "scenario": args.scenario,
        "iterations": args.iterations,
        "schema_version": 3,
        "row_grain": "operation",
        "replication_factor": 3,
        "pairs": args.pairs,
        "wfr_protocol": {
            "baseline": {"parent": 0, "child": 0, "consistency": "ALL"},
            "parent_write": {
                "client": "B", "value": 1,
                "consistency": "QUORUM", "coordinator": "node1",
            },
            "dependency_read": {
                "client": "A", "coordinator": "node1",
                "consistency": "tested read_cl",
            },
            "dependent_write": {
                "client": "A", "field": "child", "value": 1,
                "consistency": "tested write_cl",
                "coordinator": dependent_write_node,
            },
            "verification_nodes": verification_nodes,
            "verification_read_cl": "ONE",
        },
        "anomaly_criterion": (
            "dependency established (A read parent=1), dependent child=1 write "
            "succeeded, and one joint query at the same verification node "
            "observed parent=0 and child=1"
        ),
        "classification_notes": {
            "dependency_read_failure": "availability failure; not a violation",
            "dependency_not_established": "parent was not observed as 1; dependent write omitted",
            "dependent_write_failure": "availability failure; not a violation",
            "all_unavailable_under_fault": "recorded as the relevant operation failure",
            "wfr_anomaly": "possible anomaly under this experimental criterion, not a general proof",
        },
        "started_at": datetime.now().astimezone().isoformat(),
        "status": "starting",
        "recovery": "not_needed",
    }

    try:
        docker("info")
        check_ring()

        for port in (10001, 10002, 10003):
            cluster, session = connect(port, ConsistencyLevel.ONE)
            clusters.append(cluster)
            sessions.append(session)

        sessions[0].execute(
            """
            CREATE KEYSPACE IF NOT EXISTS wfr_matrix
            WITH replication = {
                'class': 'NetworkTopologyStrategy',
                'datacenter1': 3
            }
            """
        )
        sessions[0].execute(
            """
            CREATE TABLE IF NOT EXISTS wfr_matrix.samples (
                id uuid PRIMARY KEY,
                parent int,
                child int
            )
            """
        )

        baselines = [
            session.prepare(
                "INSERT INTO wfr_matrix.samples (id, parent, child) "
                "VALUES (?, ?, ?) USING TIMESTAMP ?"
            )
            for session in sessions
        ]
        parent_updates = [
            session.prepare(
                "UPDATE wfr_matrix.samples USING TIMESTAMP ? SET parent=? WHERE id=?"
            )
            for session in sessions
        ]
        child_updates = [
            session.prepare(
                "UPDATE wfr_matrix.samples USING TIMESTAMP ? SET child=? WHERE id=?"
            )
            for session in sessions
        ]
        selects = [
            session.prepare(
                "SELECT parent, child FROM wfr_matrix.samples WHERE id=?"
            )
            for session in sessions
        ]

        keys = {
            pair: [uuid.uuid4() for _ in range(args.iterations)]
            for pair in args.pairs
        }
        baseline_ts = time.time_ns() // 1000

        for group in keys.values():
            for key in group:
                statement = baselines[0].bind((key, 0, 0, baseline_ts))
                statement.consistency_level = ConsistencyLevel.ALL
                sessions[0].execute(statement)

        if args.scenario == "partition":
            if firewall("-S", CHAIN, check=False).returncode == 0:
                raise RuntimeError(
                    "Old CMDD_WFR firewall rules found. Run --recover first."
                )

        if args.scenario != "normal":
            fault_attempted = True
            metadata["recovery"] = "pending"
            if args.scenario == "partition":
                print("Creating network partition...", flush=True)
                add_partition()
            else:
                print("Stopping cassandra-3...", flush=True)
                docker("stop", "--timeout", "15", "cassandra-3")
            wait_topology(args.scenario, metadata)

        metadata["status"] = "running"
        save(folder, rows, metadata)

        for read_cl, write_cl in args.pairs:
            print(
                f"{args.scenario}: read={read_cl}, dependent-write={write_cl}",
                flush=True,
            )
            pair_name = f"{read_cl}:{write_cl}"

            for index, key in enumerate(keys[(read_cl, write_cl)]):
                context = {
                    "trial_id": f"{run_id}:{read_cl}:{write_cl}:{index + 1}",
                    "model": "WFR",
                    "scenario": args.scenario,
                    "client": "",
                    "key": str(key),
                    "_pair": pair_name,
                }
                trial_ts = baseline_ts + (index + 1) * 10

                parent_write = execute_write(
                    session=sessions[0],
                    prepared=parent_updates[0],
                    key=key,
                    value=1,
                    write_timestamp=trial_ts + 1,
                    consistency="QUORUM",
                    context=context,
                    client="B",
                    operation="PARENT_WRITE",
                    target_node="node1",
                )
                rows.append(parent_write)
                if not parent_write["success"]:
                    continue

                dependency_read = execute_dependency_read(
                    session=sessions[0],
                    prepared=selects[0],
                    key=key,
                    consistency=read_cl,
                    context=context,
                )
                rows.append(dependency_read)
                if not dependency_read["success"]:
                    continue
                if not dependency_read["_dependency_established"]:
                    continue

                dependent_write = execute_write(
                    session=sessions[dependent_write_index],
                    prepared=child_updates[dependent_write_index],
                    key=key,
                    value=1,
                    write_timestamp=trial_ts + 2,
                    consistency=write_cl,
                    context=context,
                    client="A",
                    operation="DEPENDENT_WRITE",
                    target_node=dependent_write_node,
                )
                rows.append(dependent_write)
                if not dependent_write["success"]:
                    continue

                for verify_index, verify_node in enumerate(verification_nodes):
                    verification_rows = execute_verification(
                        session=sessions[verify_index],
                        prepared=selects[verify_index],
                        key=key,
                        context=context,
                        target_node=verify_node,
                    )
                    rows.extend(verification_rows)

            save(folder, rows, metadata)

        metadata["status"] = "completed"

    except BaseException as exc:
        metadata["status"] = "aborted"
        metadata["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        try:
            if fault_attempted:
                print("Restoring cluster...", flush=True)
                if args.scenario == "partition":
                    remove_partition()
                else:
                    docker("start", "cassandra-3")
                wait_recovery()
                metadata["recovery"] = "verified"
        finally:
            for cluster in clusters:
                cluster.shutdown()
            metadata["finished_at"] = datetime.now().astimezone().isoformat()
            save(folder, rows, metadata)
            print(f"Results saved to: {folder}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario",
        choices=["normal", "node_failure", "partition"],
        default="normal",
    )
    parser.add_argument("--iterations", type=int, default=10)
    parser.add_argument("--read-cl", choices=["ONE", "QUORUM", "ALL"])
    parser.add_argument("--write-cl", choices=["ONE", "QUORUM", "ALL"])
    parser.add_argument("--recover", action="store_true")
    args = parser.parse_args()

    if args.iterations < 1:
        parser.error("--iterations must be positive")
    if bool(args.read_cl) != bool(args.write_cl):
        parser.error("Use --read-cl and --write-cl together.")

    args.pairs = (
        [(args.read_cl, args.write_cl)]
        if args.read_cl
        else PAIRS
    )

    if args.recover:
        docker("start", "cassandra-3")
        remove_partition()
        wait_recovery()
        print("Recovery verified.")
    else:
        run(args)


if __name__ == "__main__":
    main()
