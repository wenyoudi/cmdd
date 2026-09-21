"""
Monotonic-Writes (MW) consistency experiment.

Scenarios:
    normal
    node_failure
    partition

MW operation order:
    W1(x, 1) -> W2(x, 2)

The second write is issued only after the first write succeeds.
"""

import argparse
import csv
import json
import subprocess
import re
import time
import uuid

from collections import Counter
from datetime import datetime
from pathlib import Path

from cassandra import ConsistencyLevel
from cassandra.cluster import (
    Cluster,
    ExecutionProfile,
    EXEC_PROFILE_DEFAULT,
)
from cassandra.policies import (
    WhiteListRoundRobinPolicy,
    FallthroughRetryPolicy,
)


# ============================================================
# Basic settings
# ============================================================

ROOT = Path(__file__).resolve().parent

# For MW, each tuple means:
# (consistency level of W1, consistency level of W2)
PAIRS = [
    ("ONE", "ONE"),
    ("ONE", "QUORUM"),
    ("QUORUM", "ONE"),
    ("QUORUM", "QUORUM"),
    ("ALL", "ONE"),
    ("ONE", "ALL"),
]

# Separate firewall chain for our MW experiment
CHAIN = "CMDD_MW"


# Same 16-column format used by the team
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
    "write1_cl", "write2_cl", "trials", "completed_trials",
    "successful_reads", "latest_reads", "stale_prewrite_reads",
    "w1_only_reads", "trials_with_stale_state",
    "trials_with_w1_only_state", "write1_failed", "write2_failed",
    "verify_failed",
]


# ============================================================
# Connect to a Cassandra node
# ============================================================

def connect(port, level):

    profile = ExecutionProfile(
        load_balancing_policy=WhiteListRoundRobinPolicy(
            ["127.0.0.1"]
        ),
        retry_policy=FallthroughRetryPolicy(),
        consistency_level=level,
        request_timeout=15,
    )

    cluster = Cluster(
        ["127.0.0.1"],
        port=port,
        protocol_version=4,
        execution_profiles={
            EXEC_PROFILE_DEFAULT: profile
        },
    )

    try:
        return cluster, cluster.connect()

    except Exception:
        cluster.shutdown()
        raise


# ============================================================
# Docker helper
# ============================================================

def docker(*args, check=True):

    result = subprocess.run(
        ["docker", *args],
        capture_output=True,
        text=True,
        timeout=60,
    )

    if check and result.returncode:
        raise RuntimeError(
            result.stderr or result.stdout
        )

    return result


# ============================================================
# iptables helper
# ============================================================

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


# ============================================================
# Remove network partition
# ============================================================

def remove_partition():

    for hook in ("INPUT", "OUTPUT"):

        while (
            firewall(
                "-C",
                hook,
                "-j",
                CHAIN,
                check=False,
            ).returncode
            == 0
        ):

            firewall(
                "-D",
                hook,
                "-j",
                CHAIN,
            )

    exists = firewall(
        "-S",
        CHAIN,
        check=False,
    )

    if exists.returncode == 0:

        firewall(
            "-F",
            CHAIN,
        )

        firewall(
            "-X",
            CHAIN,
        )

    firewall("-S")


# ============================================================
# Create network partition
# ============================================================

def add_partition():

    firewall(
        "-N",
        CHAIN,
    )

    # Cassandra internode communication ports
    for direction in (
        "--dports",
        "--sports",
    ):

        firewall(
            "-A",
            CHAIN,
            "-p",
            "tcp",
            "-m",
            "multiport",
            direction,
            "7000,7001",
            "-j",
            "DROP",
        )

    for hook in (
        "INPUT",
        "OUTPUT",
    ):

        firewall(
            "-I",
            hook,
            "1",
            "-j",
            CHAIN,
        )


# ============================================================
# Check Cassandra ring
# ============================================================

def ring_view(node):

    result = docker(
        "exec",
        f"cassandra-{node}",
        "nodetool",
        "status",
    )

    states = re.findall(
        r"^([UD][NLJM])\s",
        result.stdout,
        re.M,
    )

    return Counter(states), result.stdout


def check_ring():

    for node in (1, 2, 3):

        states, output = ring_view(node)

        if states != Counter({"UN": 3}):

            raise RuntimeError(
                f"node{node}: "
                f"expected three UN nodes\n"
                f"{output}"
            )


# ============================================================
# Wait until fault is visible
# ============================================================

def wait_topology(scenario, evidence):

    deadline = time.monotonic() + 180

    while True:

        nodes = (
            (1, 2, 3)
            if scenario == "partition"
            else (1, 2)
        )

        views = {
            n: ring_view(n)
            for n in nodes
        }

        # node1 and node2 should see:
        # two nodes UP, one node DOWN
        good = all(
            views[n][0]
            == Counter({
                "UN": 2,
                "DN": 1,
            })
            for n in (1, 2)
        )

        # In a partition, isolated node3 sees itself,
        # but cannot see node1/node2.
        if scenario == "partition":

            good = (
                good
                and views[3][0]
                == Counter({
                    "UN": 1,
                    "DN": 2,
                })
            )

        if good:

            evidence["fault_views"] = {
                str(n): v[1]
                for n, v in views.items()
            }

            return

        if time.monotonic() > deadline:

            raise RuntimeError(
                "Fault topology not confirmed"
            )

        time.sleep(3)


# ============================================================
# Wait for recovery
# ============================================================

def wait_recovery():

    deadline = time.monotonic() + 300

    while True:

        try:

            check_ring()

            for port in (
                10001,
                10002,
                10003,
            ):

                cluster, session = connect(
                    port,
                    ConsistencyLevel.ONE,
                )

                try:

                    session.execute(
                        "SELECT release_version "
                        "FROM system.local"
                    )

                finally:

                    cluster.shutdown()

            return

        except Exception:

            if time.monotonic() > deadline:

                raise RuntimeError(
                    "Recovery not confirmed"
                )

            time.sleep(3)


# ============================================================
# Create one result row
# ============================================================

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
        timestamp=(
            datetime.now()
            .astimezone()
            .isoformat()
        ),
        error="",
    )


# ============================================================
# Execute a WRITE
# ============================================================

def execute_write(
    session,
    prepared,
    key,
    version,
    write_timestamp,
    consistency,
    context,
    target_node,
):

    row = make_row(
        context,
        operation="WRITE",
        target_node=target_node,
        version_written=version,
        write_cl=consistency,
    )

    started = time.perf_counter()

    try:

        statement = prepared.bind(
            (
                key,
                version,
                write_timestamp,
            )
        )

        statement.consistency_level = getattr(
            ConsistencyLevel,
            consistency,
        )

        session.execute(statement)

        row["success"] = True

    except Exception as exc:

        row["error"] = (
            f"{type(exc).__name__}: {exc}"
        )

    finally:

        row["latency_ms"] = round(
            (
                time.perf_counter()
                - started
            )
            * 1000,
            3,
        )

    return row


# ============================================================
# Verification READ
# ============================================================

def execute_read(session, prepared, key, context, target_node, expected_version):
    row = make_row(
        context, operation="READ", target_node=target_node, read_cl="ONE"
    )
    started = time.perf_counter()
    try:
        statement = prepared.bind((key,))
        statement.consistency_level = ConsistencyLevel.ONE
        result = session.execute(statement)
        record = result.one()
        observed = record.version if record else None
        row["version_observed"] = observed if observed is not None else ""
        row["success"] = True

        # Experimental MW observation semantics:
        # 2 = latest state after W1 and W2.
        # 0 = stale/pre-write state; replica divergence, not MW reversal.
        # 1 = W1 visible but W2 absent after both writes succeeded;
        #     record as a possible MW ordering anomaly for this experiment.
        # Keep the common team's "violation" column, but interpret True in
        # the report as an observed anomaly, not proof of a general guarantee.
        row["violation"] = (observed == 1)
    except Exception as exc:
        row["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        row["latency_ms"] = round((time.perf_counter() - started) * 1000, 3)
    return row


# ============================================================
# Produce summary.csv
# ============================================================

def summarize(rows):
    output = []
    pair_names = []
    for row in rows:
        pair_name = row.get("_pair")
        if pair_name and pair_name not in pair_names:
            pair_names.append(pair_name)

    for pair_name in pair_names:
        write1_cl, write2_cl = pair_name.split(":")
        group = [row for row in rows if row.get("_pair") == pair_name]
        trial_ids = sorted({row["trial_id"] for row in group})
        write1_rows = [row for row in group if row["operation"] == "WRITE" and row["version_written"] == 1]
        write2_rows = [row for row in group if row["operation"] == "WRITE" and row["version_written"] == 2]
        reads = [row for row in group if row["operation"] == "READ"]
        successful_reads = [row for row in reads if row["success"]]

        latest_reads = sum(row["version_observed"] == 2 for row in successful_reads)
        stale_prewrite_reads = sum(row["version_observed"] == 0 for row in successful_reads)
        w1_only_reads = sum(row["version_observed"] == 1 for row in successful_reads)

        completed_trials = 0
        trials_with_stale_state = 0
        trials_with_w1_only_state = 0

        for trial_id in trial_ids:
            trial_rows = [row for row in group if row["trial_id"] == trial_id]
            trial_w1 = [row for row in trial_rows if row["operation"] == "WRITE" and row["version_written"] == 1]
            trial_w2 = [row for row in trial_rows if row["operation"] == "WRITE" and row["version_written"] == 2]
            trial_reads = [row for row in trial_rows if row["operation"] == "READ"]

            expected_reads = 2 if trial_rows[0]["scenario"] == "node_failure" else 3

            if (len(trial_w1) == 1 and trial_w1[0]["success"]
                    and len(trial_w2) == 1 and trial_w2[0]["success"]
                    and len(trial_reads) == expected_reads
                    and all(row["success"] for row in trial_reads)):
                completed_trials += 1

            observed_versions = {row["version_observed"] for row in trial_reads if row["success"]}
            if 0 in observed_versions:
                trials_with_stale_state += 1
            if 1 in observed_versions:
                trials_with_w1_only_state += 1

        output.append({
            "write1_cl": write1_cl,
            "write2_cl": write2_cl,
            "trials": len(trial_ids),
            "completed_trials": completed_trials,
            "successful_reads": len(successful_reads),
            "latest_reads": latest_reads,
            "stale_prewrite_reads": stale_prewrite_reads,
            "w1_only_reads": w1_only_reads,
            "trials_with_stale_state": trials_with_stale_state,
            "trials_with_w1_only_state": trials_with_w1_only_state,
            "write1_failed": sum(not row["success"] for row in write1_rows),
            "write2_failed": sum(not row["success"] for row in write2_rows),
            "verify_failed": sum(not row["success"] for row in reads),
        })
    return output


# ============================================================
# Save CSV + metadata
# ============================================================

def save(folder, rows, metadata):

    # _pair is internal.
    # Do not put it into team's trials.csv.
    clean_rows = []

    for row in rows:

        clean_rows.append({
            field: row.get(field, "")
            for field in TRIAL_FIELDS
        })

    with (
        folder / "trials.csv"
    ).open(
        "w",
        newline="",
        encoding="utf-8-sig",
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=TRIAL_FIELDS,
        )

        writer.writeheader()

        writer.writerows(
            clean_rows
        )

    with (
        folder / "summary.csv"
    ).open(
        "w",
        newline="",
        encoding="utf-8-sig",
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=SUMMARY_FIELDS,
        )

        writer.writeheader()

        writer.writerows(
            summarize(rows)
        )

    (
        folder / "metadata.json"
    ).write_text(
        json.dumps(
            metadata,
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


# ============================================================
# Main MW experiment
# ============================================================

def run(args):

    run_id = str(uuid.uuid4())

    # Put MW results in the repository-level results directory.
    results_root = (
        ROOT.parent.parent
        / "results"
        / "monotonic_writes"
    )

    folder = (
        results_root
        / f"{args.scenario}_{run_id}"
    )

    folder.mkdir(
        parents=True,
        exist_ok=True,
    )

    rows = []

    clusters = []

    sessions = []

    fault_attempted = False

    verification_nodes = (
        ["node1", "node2"]
        if args.scenario == "node_failure"
        else ["node1", "node2", "node3"]
    )

    metadata = {
        "run_id": run_id,
        "model": "MW",
        "scenario": args.scenario,
        "iterations": args.iterations,
        "schema_version": 3,
        "row_grain": "operation",
        "replication_factor": 3,
        "pairs": args.pairs,
        "mw_protocol": {
            "baseline_version": 0,
            "write1_version": 1,
            "write2_version": 2,
            "write1_coordinator": "node1",
            "write2_coordinator": "node2",
            "verification_nodes": verification_nodes,
            "verification_read_cl": "ONE",
        },
        "observation_semantics": {
            "version_2": "expected latest state after W1 and W2",
            "version_0": "stale pre-write replica state; not classified as an MW ordering violation",
            "version_1": "possible MW ordering anomaly under the experimental criterion",
        },
        "timestamp_design": {
            "baseline": "baseline_ts",
            "write1": "baseline_ts + 1",
            "write2": "baseline_ts + 2",
            "limitation": (
                "Explicit increasing Cassandra timestamps encode the intended W1->W2 order. "
                "Cassandra timestamp-based last-write-wins conflict resolution may help preserve "
                "this order, so absence of an observed reversal is not proof of a general MW guarantee."
            ),
        },
        "started_at": datetime.now().astimezone().isoformat(),
        "status": "starting",
        "recovery": "not_needed",
    }

    try:

        # ====================================================
        # STEP 1: check Docker and Cassandra
        # ====================================================

        docker("info")

        check_ring()

        # ====================================================
        # STEP 2: connect to node1 / node2 / node3
        # ====================================================

        for port in (
            10001,
            10002,
            10003,
        ):

            cluster, session = connect(
                port,
                ConsistencyLevel.ONE,
            )

            clusters.append(cluster)

            sessions.append(session)

        # sessions[0] -> node1
        # sessions[1] -> node2
        # sessions[2] -> node3

        # ====================================================
        # STEP 3: create keyspace and table
        # ====================================================

        sessions[0].execute(
            """
            CREATE KEYSPACE IF NOT EXISTS mw_matrix
            WITH replication = {
                'class': 'NetworkTopologyStrategy',
                'datacenter1': 3
            }
            """
        )

        sessions[0].execute(
            """
            CREATE TABLE IF NOT EXISTS
            mw_matrix.samples (
                id uuid PRIMARY KEY,
                version int
            )
            """
        )

        # ====================================================
        # STEP 4: prepare CQL
        # ====================================================

        inserts = [
            session.prepare(
                """
                INSERT INTO mw_matrix.samples
                (id, version)
                VALUES (?, ?)
                USING TIMESTAMP ?
                """
            )
            for session in sessions
        ]

        selects = [
            session.prepare(
                """
                SELECT version
                FROM mw_matrix.samples
                WHERE id=?
                """
            )
            for session in sessions
        ]

        # ====================================================
        # STEP 5: generate independent keys
        # ====================================================

        keys = {
            pair: [
                uuid.uuid4()
                for _ in range(
                    args.iterations
                )
            ]
            for pair in args.pairs
        }

        baseline_ts = (
            time.time_ns()
            // 1000
        )

        # ====================================================
        # STEP 6: baseline version = 0
        # ====================================================

        for group in keys.values():

            for key in group:

                statement = (
                    inserts[0].bind(
                        (
                            key,
                            0,
                            baseline_ts,
                        )
                    )
                )

                statement.consistency_level = (
                    ConsistencyLevel.ALL
                )

                sessions[0].execute(
                    statement
                )

        # ====================================================
        # STEP 7: inject fault
        # ====================================================

        if args.scenario == "partition":

            # Make sure old MW firewall rules
            # are not accidentally left behind.
            if (
                firewall(
                    "-S",
                    CHAIN,
                    check=False,
                ).returncode
                == 0
            ):

                raise RuntimeError(
                    "Old CMDD_MW firewall "
                    "rules found. "
                    "Run --recover first."
                )

        if args.scenario != "normal":

            fault_attempted = True

            metadata["recovery"] = (
                "pending"
            )

            if args.scenario == "partition":

                print(
                    "Creating network partition...",
                    flush=True,
                )

                add_partition()

            else:

                print(
                    "Stopping cassandra-3...",
                    flush=True,
                )

                docker(
                    "stop",
                    "--timeout",
                    "15",
                    "cassandra-3",
                )

            wait_topology(
                args.scenario,
                metadata,
            )

        metadata["status"] = "running"

        save(
            folder,
            rows,
            metadata,
        )

        # ====================================================
        # STEP 8: actual MW experiment
        # ====================================================

        for write1_cl, write2_cl in args.pairs:

            print(
                f"{args.scenario}: "
                f"W1={write1_cl}, "
                f"W2={write2_cl}",
                flush=True,
            )

            pair_name = (
                f"{write1_cl}:"
                f"{write2_cl}"
            )

            for index, key in enumerate(
                keys[
                    (
                        write1_cl,
                        write2_cl,
                    )
                ]
            ):

                context = {
                    "trial_id": (
                        f"{run_id}:"
                        f"{write1_cl}:"
                        f"{write2_cl}:"
                        f"{index + 1}"
                    ),
                    "model": "MW",
                    "scenario": args.scenario,
                    "client": "A",
                    "key": str(key),
                    "_pair": pair_name,
                }

                # ============================================
                # W1
                #
                # Client A writes version 1 through node1.
                # ============================================

                write1 = execute_write(
                    session=sessions[0],
                    prepared=inserts[0],
                    key=key,
                    version=1,
                    write_timestamp=(
                        baseline_ts + 1
                    ),
                    consistency=write1_cl,
                    context=context,
                    target_node="node1",
                )

                rows.append(write1)

                # IMPORTANT:
                # W2 is only issued after W1 succeeds.
                if not write1["success"]:
                    continue

                # ============================================
                # W2
                #
                # Same logical Client A,
                # but use node2 as coordinator.
                #
                # version 1 -> version 2
                # ============================================

                write2 = execute_write(
                    session=sessions[1],
                    prepared=inserts[1],
                    key=key,
                    version=2,
                    write_timestamp=(
                        baseline_ts + 2
                    ),
                    consistency=write2_cl,
                    context=context,
                    target_node="node2",
                )

                rows.append(write2)

                if not write2["success"]:
                    continue

                # ============================================
                # Verification
                # ============================================

                # ============================================
                # Verification reads

                # After W1 and W2 both succeed, query every
                # reachable replica/coordinator separately.

                # node1 -> port 10001
                # node2 -> port 10002
                # node3 -> port 10003
                #
                # This lets us observe whether different
                # parts of the cluster expose different
                # versions after the two writes.
                # ============================================

                for verify_index, verify_node in enumerate(verification_nodes):

                    verify = execute_read(
                        session=sessions[verify_index],
                        prepared=selects[verify_index],
                        key=key,
                        context=context,
                        target_node=verify_node,
                        expected_version=2,
                    )

                    rows.append(verify)

            save(
                folder,
                rows,
                metadata,
            )

        metadata["status"] = "completed"

    except BaseException as exc:

        metadata["status"] = "aborted"

        metadata["error"] = (
            f"{type(exc).__name__}: {exc}"
        )

        raise

    finally:

        # ====================================================
        # STEP 9: recover cluster
        # ====================================================

        try:

            if fault_attempted:

                print(
                    "Restoring cluster...",
                    flush=True,
                )

                if args.scenario == "partition":

                    remove_partition()

                else:

                    docker(
                        "start",
                        "cassandra-3",
                    )

                wait_recovery()

                metadata["recovery"] = (
                    "verified"
                )

        finally:

            for cluster in clusters:

                cluster.shutdown()

            metadata["finished_at"] = (
                datetime.now()
                .astimezone()
                .isoformat()
            )

            save(
                folder,
                rows,
                metadata,
            )

            print(
                f"Results saved to: {folder}",
                flush=True,
            )


# ============================================================
# Command line
# ============================================================

def main():

    parser = argparse.ArgumentParser(
        description=__doc__
    )

    parser.add_argument(
        "--scenario",
        choices=[
            "normal",
            "node_failure",
            "partition",
        ],
        default="normal",
    )

    parser.add_argument(
        "--iterations",
        type=int,
        default=10,
    )

    parser.add_argument(
        "--write1-cl",
        choices=[
            "ONE",
            "QUORUM",
            "ALL",
        ],
    )

    parser.add_argument(
        "--write2-cl",
        choices=[
            "ONE",
            "QUORUM",
            "ALL",
        ],
    )

    parser.add_argument(
        "--recover",
        action="store_true",
    )

    args = parser.parse_args()

    if args.iterations < 1:

        parser.error(
            "--iterations must be positive"
        )

    # Either specify both CLs,
    # or specify neither and run all six pairs.
    if (
        bool(args.write1_cl)
        != bool(args.write2_cl)
    ):

        parser.error(
            "Use --write1-cl and "
            "--write2-cl together."
        )

    if args.write1_cl:

        args.pairs = [
            (
                args.write1_cl,
                args.write2_cl,
            )
        ]

    else:

        args.pairs = PAIRS

    # Emergency/manual recovery command
    if args.recover:

        docker(
            "start",
            "cassandra-3",
        )

        remove_partition()

        wait_recovery()

        print(
            "Recovery verified."
        )

    else:

        run(args)


if __name__ == "__main__":
    main()
