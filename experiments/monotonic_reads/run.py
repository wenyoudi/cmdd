"""
Monotonic Reads Consistency Experiment
======================================

Tests whether one logical client ever observes an older version after
it has already observed a newer version.

Monotonic-read violation:

    current_observed_version < highest_version_previously_observed

The same workload supports three scenarios:

    normal
    node_failure
    network_partition

Fault injection itself is intentionally external to this script so that
Docker/network operations are explicit and reproducible.

Cluster:
    cassandra-1 -> localhost:10001
    cassandra-2 -> localhost:10002
    cassandra-3 -> localhost:10003
"""

import argparse
import csv
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement


# =====================================================================
# Configuration
# =====================================================================

NODES = {
    "cassandra-1": ("127.0.0.1", 10001),
    "cassandra-2": ("127.0.0.1", 10002),
    "cassandra-3": ("127.0.0.1", 10003),
}

OUTPUT_COLUMNS = [
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

CL_MAP = {
    "ONE": ConsistencyLevel.ONE,
    "TWO": ConsistencyLevel.TWO,
    "THREE": ConsistencyLevel.THREE,
    "QUORUM": ConsistencyLevel.QUORUM,
    "ALL": ConsistencyLevel.ALL,
    "LOCAL_ONE": ConsistencyLevel.LOCAL_ONE,
    "LOCAL_QUORUM": ConsistencyLevel.LOCAL_QUORUM,
}


# =====================================================================
# General helpers
# =====================================================================

def utc_timestamp():
    """Return an ISO-8601 UTC timestamp."""
    return datetime.now(timezone.utc).isoformat()


def format_error(exc):
    """Return a compact error string suitable for the CSV."""
    return f"{type(exc).__name__}: {exc}"


# =====================================================================
# Cassandra connection helpers
# =====================================================================

def connect_to_node(node_name):
    """
    Connect directly to exactly one Cassandra coordinator.

    Each Docker Cassandra container is exposed through a different
    localhost port, so a separate Cluster object is used for each node.

    We intentionally do not give the driver the addresses of the other
    nodes. This lets target_node accurately describe the coordinator
    selected by the experiment.
    """
    host, port = NODES[node_name]

    cluster = Cluster(
        [host],
        port=port,
        connect_timeout=5,
        control_connection_timeout=5,
    )

    session = cluster.connect()
    return cluster, session


def try_connect(node_name):
    """
    Attempt to connect to a node.

    Returns:
        (cluster, session, error)

    If the connection fails:
        (None, None, exception)
    """
    try:
        cluster, session = connect_to_node(node_name)
        return cluster, session, None

    except Exception as exc:
        return None, None, exc


def setup_schema():
    """
    Create the experiment keyspace and table.

    Replication factor = 3, so the experiment row is replicated across
    all three Cassandra nodes.
    """
    cluster, session = connect_to_node("cassandra-1")

    try:
        session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS consistency_test
            WITH replication = {
                'class': 'NetworkTopologyStrategy',
                'datacenter1': 3
            }
            """
        )

        session.execute(
            """
            CREATE TABLE IF NOT EXISTS
            consistency_test.monotonic_reads (
                key text PRIMARY KEY,
                version bigint,
                writer text,
                updated_at timestamp
            )
            """
        )

    finally:
        cluster.shutdown()


# =====================================================================
# CSV logger
# =====================================================================

class CSVLogger:
    """
    Thread-safe CSV logger shared by the writer and reader.
    """

    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

        self.lock = threading.Lock()

        self.file = open(
            self.path,
            "w",
            newline="",
            encoding="utf-8",
        )

        self.writer = csv.DictWriter(
            self.file,
            fieldnames=OUTPUT_COLUMNS,
        )

        self.writer.writeheader()
        self.file.flush()

    def log(self, row):
        with self.lock:
            self.writer.writerow(row)
            self.file.flush()

    def close(self):
        self.file.close()


# =====================================================================
# Writer
# =====================================================================

def writer_loop(
    stop_event,
    logger,
    trial_id,
    scenario,
    key,
    write_cl,
    write_interval,
):
    """
    Continuously write increasing versions through cassandra-1.

        v1
        v2
        v3
        ...

    cassandra-1 is intentionally kept as the writer coordinator.

    The reader, in contrast, rotates between all three coordinators.
    """

    cluster = None
    session = None

    statement = SimpleStatement(
        """
        INSERT INTO consistency_test.monotonic_reads
        (key, version, writer, updated_at)
        VALUES (%s, %s, %s, toTimestamp(now()))
        """,
        consistency_level=CL_MAP[write_cl],
    )

    version = 0

    try:
        while not stop_event.is_set():

            # ---------------------------------------------------------
            # Reconnect if necessary
            # ---------------------------------------------------------

            if session is None:
                try:
                    cluster, session = connect_to_node("cassandra-1")

                except Exception as exc:
                    logger.log({
                        "trial_id": trial_id,
                        "model": "monotonic_reads",
                        "scenario": scenario,
                        "client": "writer-1",
                        "operation": "write",
                        "key": key,
                        "version_written": version + 1,
                        "version_observed": "",
                        "read_cl": "",
                        "write_cl": write_cl,
                        "target_node": "cassandra-1",
                        "success": False,
                        "violation": False,
                        "latency_ms": 0,
                        "timestamp": utc_timestamp(),
                        "error": format_error(exc),
                    })

                    time.sleep(write_interval)
                    continue

            # ---------------------------------------------------------
            # Write next version
            # ---------------------------------------------------------

            next_version = version + 1

            start = time.perf_counter()

            success = True
            error = ""

            try:
                session.execute(
                    statement,
                    (
                        key,
                        next_version,
                        "writer-1",
                    ),
                )

                version = next_version

            except Exception as exc:
                success = False
                error = format_error(exc)

                # Discard the connection so the next iteration tries
                # to establish a fresh one.
                try:
                    if cluster is not None:
                        cluster.shutdown()
                except Exception:
                    pass

                cluster = None
                session = None

            latency_ms = (
                time.perf_counter() - start
            ) * 1000

            logger.log({
                "trial_id": trial_id,
                "model": "monotonic_reads",
                "scenario": scenario,
                "client": "writer-1",
                "operation": "write",
                "key": key,
                "version_written": next_version,
                "version_observed": "",
                "read_cl": "",
                "write_cl": write_cl,
                "target_node": "cassandra-1",
                "success": success,
                "violation": False,
                "latency_ms": round(latency_ms, 3),
                "timestamp": utc_timestamp(),
                "error": error,
            })

            time.sleep(write_interval)

    finally:
        try:
            if cluster is not None:
                cluster.shutdown()
        except Exception:
            pass


# =====================================================================
# Reader connection manager
# =====================================================================

class NodeConnectionManager:
    """
    Maintains direct connections to Cassandra nodes.

    A key difference from the original implementation is that a failed
    node is NOT permanently removed from the experiment.

    If cassandra-3 is stopped and later restarted, subsequent reads will
    attempt to reconnect to it automatically.
    """

    def __init__(self):
        self.connections = {}

    def get_session(self, node_name):
        """
        Return a session for node_name.

        If no usable connection exists, try to create one.
        """

        if node_name in self.connections:
            cluster, session = self.connections[node_name]
            return session

        cluster, session, error = try_connect(node_name)

        if error is not None:
            raise error

        self.connections[node_name] = (
            cluster,
            session,
        )

        return session

    def invalidate(self, node_name):
        """
        Remove a failed connection so the next access reconnects.
        """

        connection = self.connections.pop(
            node_name,
            None,
        )

        if connection is None:
            return

        cluster, _ = connection

        try:
            cluster.shutdown()
        except Exception:
            pass

    def close_all(self):
        for cluster, _ in self.connections.values():
            try:
                cluster.shutdown()
            except Exception:
                pass

        self.connections.clear()


# =====================================================================
# Reader
# =====================================================================

def run_reader(
    logger,
    trial_id,
    scenario,
    key,
    read_cl,
    write_cl,
    duration,
    read_interval,
):
    """
    Run one logical reader for `duration` seconds.

    Coordinator sequence:

        cassandra-1
        cassandra-2
        cassandra-3
        cassandra-1
        ...

    IMPORTANT:

    The client identity remains "reader-1".

    Changing coordinator does NOT mean changing logical client.

    ---------------------------------------------------------------
    Monotonic-read condition
    ---------------------------------------------------------------

    If reader-1 has already observed version 20, then any subsequent
    successful read returning:

        20, 21, 22, ...

    is consistent with monotonic reads.

    A subsequent read returning:

        19

    is a violation.

    We therefore maintain a high-water mark:

        max_version_seen

    and detect:

        observed_version < max_version_seen
    """

    manager = NodeConnectionManager()

    node_sequence = list(NODES.keys())

    statement = SimpleStatement(
        """
        SELECT version
        FROM consistency_test.monotonic_reads
        WHERE key = %s
        """,
        consistency_level=CL_MAP[read_cl],
    )

    max_version_seen = None

    violation_count = 0
    successful_reads = 0
    failed_reads = 0
    total_reads = 0

    start_time = time.monotonic()

    try:
        while time.monotonic() - start_time < duration:

            node_name = node_sequence[
                total_reads % len(node_sequence)
            ]

            total_reads += 1

            observed_version = None
            previous_max = max_version_seen

            success = True
            violation = False
            error = ""

            start = time.perf_counter()

            try:
                # -----------------------------------------------------
                # Get/re-establish connection
                # -----------------------------------------------------

                session = manager.get_session(node_name)

                # -----------------------------------------------------
                # Execute read
                # -----------------------------------------------------

                result = session.execute(
                    statement,
                    (key,),
                )

                row = result.one()

                if row is not None:
                    observed_version = row.version
                    successful_reads += 1

                    # -------------------------------------------------
                    # Monotonic-read check
                    # -------------------------------------------------

                    if (
                        max_version_seen is not None
                        and observed_version < max_version_seen
                    ):
                        violation = True
                        violation_count += 1

                    # Maintain the client's high-water mark.
                    if (
                        max_version_seen is None
                        or observed_version > max_version_seen
                    ):
                        max_version_seen = observed_version

            except Exception as exc:
                success = False
                failed_reads += 1
                error = format_error(exc)

                # Force reconnection next time this coordinator is used.
                manager.invalidate(node_name)

            latency_ms = (
                time.perf_counter() - start
            ) * 1000

            logger.log({
                "trial_id": trial_id,
                "model": "monotonic_reads",
                "scenario": scenario,
                "client": "reader-1",
                "operation": "read",
                "key": key,
                "version_written": "",
                "version_observed": (
                    observed_version
                    if observed_version is not None
                    else ""
                ),
                "read_cl": read_cl,
                "write_cl": write_cl,
                "target_node": node_name,
                "success": success,
                "violation": violation,
                "latency_ms": round(latency_ms, 3),
                "timestamp": utc_timestamp(),
                "error": error,
            })

            # if violation:
            #     print()
            #     print("!!! MONOTONIC-READ VIOLATION !!!")
            #     print(f"Coordinator:       {node_name}")
            #     print(f"Previously seen:   {previous_max}")
            #     print(f"Currently observed:{observed_version}")
            #     print()

            time.sleep(read_interval)

    finally:
        manager.close_all()

    return {
        "total_reads": total_reads,
        "successful_reads": successful_reads,
        "failed_reads": failed_reads,
        "violations": violation_count,
        "max_version_seen": max_version_seen,
    }


# =====================================================================
# Initial row
# =====================================================================

def initialise_row(key, write_cl):
    """
    Initialise the experiment row to version 0.

    This prevents an absent row from being confused with a stale read.
    """

    cluster, session = connect_to_node("cassandra-1")

    statement = SimpleStatement(
        """
        INSERT INTO consistency_test.monotonic_reads
        (key, version, writer, updated_at)
        VALUES (%s, %s, %s, toTimestamp(now()))
        """,
        consistency_level=CL_MAP[write_cl],
    )

    try:
        session.execute(
            statement,
            (
                key,
                0,
                "initializer",
            ),
        )

    finally:
        cluster.shutdown()


# =====================================================================
# Scenario instructions
# =====================================================================

def print_scenario_instructions(scenario):
    print()
    print("-" * 70)

    if scenario == "normal":
        print("SCENARIO: NORMAL OPERATION")
        print()
        print("Keep all three Cassandra nodes running.")
        print("No manual action is required during this trial.")

    elif scenario == "node_failure":
        print("SCENARIO: NODE FAILURE / RECOVERY")
        print()
        print("Use another terminal during the experiment.")
        print()
        print("After the experiment starts, stop cassandra-3:")
        print()
        print("    docker stop cassandra-3")
        print()
        print("Allow writes to continue while it is offline.")
        print()
        print("Then restart cassandra-3:")
        print()
        print("    docker start cassandra-3")
        print()
        print("The reader will automatically try to reconnect.")

    elif scenario == "network_partition":
        print("SCENARIO: NETWORK PARTITION")
        print()
        print("Introduce the network partition from another terminal.")
        print()
        print("Keep the partition active while writes continue,")
        print("then heal it before the experiment finishes.")
        print()
        print(
            "Use your group's documented partition commands so that "
            "the exact procedure is reproducible."
        )

    print("-" * 70)
    print()


# =====================================================================
# Countdown
# =====================================================================

def countdown(seconds):
    """
    Give the user time to switch to the fault-injection terminal.
    """

    if seconds <= 0:
        return

    print(
        f"Workload starts in {seconds} seconds..."
    )

    for remaining in range(seconds, 0, -1):
        print(
            f"\rStarting in {remaining:2d}s ",
            end="",
            flush=True,
        )
        time.sleep(1)

    print("\rStarting now!      ")


# =====================================================================
# Trial
# =====================================================================

def run_trial(args):
    trial_id = str(uuid.uuid4())
    key = f"mr-{trial_id}"

    print()
    print("=" * 70)
    print("MONOTONIC READS EXPERIMENT")
    print("=" * 70)

    print(f"Trial ID:       {trial_id}")
    print(f"Scenario:       {args.scenario}")
    print(f"Read CL:        {args.read_cl}")
    print(f"Write CL:       {args.write_cl}")
    print(f"Duration:       {args.duration} seconds")
    print(f"Read interval:  {args.read_interval} seconds")
    print(f"Write interval: {args.write_interval} seconds")
    print(f"Output:         {args.output}")

    print_scenario_instructions(
        args.scenario
    )

    logger = CSVLogger(args.output)

    stop_event = threading.Event()
    writer = None

    try:
        # -------------------------------------------------------------
        # Establish version 0 while cluster should still be healthy.
        # -------------------------------------------------------------

        print("Initialising experiment row...")
        initialise_row(
            key,
            args.write_cl,
        )

        print("Initial row created at version 0.")

        # -------------------------------------------------------------
        # Give the initial value a small convergence period.
        # -------------------------------------------------------------

        if args.warmup > 0:
            print(
                f"Waiting {args.warmup}s before workload..."
            )
            time.sleep(args.warmup)

        # -------------------------------------------------------------
        # Countdown
        # -------------------------------------------------------------

        countdown(args.countdown)

        # -------------------------------------------------------------
        # Start writer
        # -------------------------------------------------------------

        writer = threading.Thread(
            target=writer_loop,
            args=(
                stop_event,
                logger,
                trial_id,
                args.scenario,
                key,
                args.write_cl,
                args.write_interval,
            ),
            daemon=True,
        )

        writer.start()

        # Give writer a brief head start.
        time.sleep(args.writer_head_start)

        # -------------------------------------------------------------
        # Start reader
        # -------------------------------------------------------------

        if args.ready_file:
            ready_path = Path(args.ready_file)
            ready_path.parent.mkdir(parents=True, exist_ok=True)
            ready_path.touch()

        print()
        print("Reader started.")
        print(
            f"Experiment will run for "
            f"{args.duration} seconds."
        )
        print()

        results = run_reader(
            logger=logger,
            trial_id=trial_id,
            scenario=args.scenario,
            key=key,
            read_cl=args.read_cl,
            write_cl=args.write_cl,
            duration=args.duration,
            read_interval=args.read_interval,
        )

    finally:
        stop_event.set()

        if writer is not None:
            writer.join(timeout=10)

        logger.close()

    # -----------------------------------------------------------------
    # Results
    # -----------------------------------------------------------------

    print()
    print("=" * 70)
    print("RESULT")
    print("=" * 70)

    print(
        f"Total read attempts: {results['total_reads']}"
    )

    print(
        f"Successful reads:    {results['successful_reads']}"
    )

    print(
        f"Failed reads:        {results['failed_reads']}"
    )

    print(
        f"Violations:          {results['violations']}"
    )

    if results["successful_reads"] > 0:
        violation_rate = (
            results["violations"]
            / results["successful_reads"]
        )

        print(
            f"Violation rate:      "
            f"{violation_rate:.4%}"
        )

    print(
        f"Highest version seen:"
        f" {results['max_version_seen']}"
    )

    print(f"CSV output:          {args.output}")

    print("=" * 70)
    print()


# =====================================================================
# Command-line arguments
# =====================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Test Cassandra monotonic-read consistency "
            "under normal and fault scenarios."
        )
    )

    parser.add_argument(
        "--scenario",
        choices=[
            "normal",
            "node_failure",
            "network_partition",
        ],
        default="normal",
        help="Experimental scenario.",
    )

    parser.add_argument(
        "--read-cl",
        choices=CL_MAP.keys(),
        default="ONE",
        help="Cassandra read consistency level.",
    )

    parser.add_argument(
        "--write-cl",
        choices=CL_MAP.keys(),
        default="ONE",
        help="Cassandra write consistency level.",
    )

    parser.add_argument(
        "--duration",
        type=float,
        default=30,
        help=(
            "Reader workload duration in seconds. "
            "Default: 30."
        ),
    )

    parser.add_argument(
        "--read-interval",
        type=float,
        default=0.01,
        help=(
            "Seconds between sequential reads. "
            "Default: 0.01."
        ),
    )

    parser.add_argument(
        "--write-interval",
        type=float,
        default=0.02,
        help=(
            "Seconds between writes. "
            "Default: 0.02."
        ),
    )

    parser.add_argument(
        "--warmup",
        type=float,
        default=1,
        help=(
            "Seconds to wait after creating version 0. "
            "Default: 1."
        ),
    )

    parser.add_argument(
        "--countdown",
        type=int,
        default=3,
        help=(
            "Countdown before workload starts. "
            "Default: 3."
        ),
    )

    parser.add_argument(
        "--writer-head-start",
        type=float,
        default=0.5,
        help=(
            "Seconds the writer runs before reader starts. "
            "Default: 0.5."
        ),
    )

    parser.add_argument(
        "--output",
        default="results/monotonic_reads.csv",
        help="CSV output path.",
    )

    parser.add_argument(
        "--ready-file",
        default=None,
        help=(
            "Optional path touched immediately before the reader workload "
            "starts. Used by orchestration scripts to synchronize fault timing."
        ),
    )

    return parser.parse_args()


# =====================================================================
# Main
# =====================================================================

if __name__ == "__main__":
    args = parse_args()

    setup_schema()
    run_trial(args)

    