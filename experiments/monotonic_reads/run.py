import csv
import time
from datetime import datetime, timezone

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster


# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------

KEYSPACE = "consistency_test"
TABLE = "products"

PRODUCT_ID = "P0001"
CLIENT = "client_A"

TRIALS = 100

# Change this for each experiment:
SCENARIO = "normal"

# ONE, QUORUM, or ALL
READ_CL_NAME = "ONE"

OUTPUT_FILE = (
    f"results/raw/mr_{SCENARIO}_{READ_CL_NAME.lower()}.csv"
)


# -------------------------------------------------------------------
# Cassandra consistency levels
# -------------------------------------------------------------------

CONSISTENCY_LEVELS = {
    "ONE": ConsistencyLevel.ONE,
    "QUORUM": ConsistencyLevel.QUORUM,
    "ALL": ConsistencyLevel.ALL,
}


# -------------------------------------------------------------------
# Cassandra connection
# -------------------------------------------------------------------

def connect(port):
    """
    Connect to one specific Cassandra node through its exposed
    Docker host port.
    """
    cluster = Cluster(
        ["127.0.0.1"],
        port=port
    )

    session = cluster.connect(KEYSPACE)

    return cluster, session


# -------------------------------------------------------------------
# Read operation
# -------------------------------------------------------------------

def read_product(session, consistency_level):
    query = session.prepare(
        f"""
        SELECT product_id, stock, version
        FROM {TABLE}
        WHERE product_id = ?
        """
    )

    query.consistency_level = consistency_level

    start = time.perf_counter()

    try:
        row = session.execute(
            query,
            [PRODUCT_ID]
        ).one()

        latency_ms = (
            time.perf_counter() - start
        ) * 1000

        if row is None:
            return None, latency_ms, False, "No row returned"

        return row.version, latency_ms, True, ""

    except Exception as exc:
        latency_ms = (
            time.perf_counter() - start
        ) * 1000

        return None, latency_ms, False, str(exc)


# -------------------------------------------------------------------
# CSV logging
# -------------------------------------------------------------------

FIELDS = [
    "trial_id",
    "model",
    "scenario",
    "client",
    "operation",
    "product_id",
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


def log_result(writer, trial_id, node, version,
               success, violation, latency, error):

    writer.writerow({
        "trial_id": trial_id,
        "model": "MR",
        "scenario": SCENARIO,
        "client": CLIENT,
        "operation": "READ",
        "product_id": PRODUCT_ID,
        "version_written": "",
        "version_observed": version if version is not None else "",
        "read_cl": READ_CL_NAME,
        "write_cl": "",
        "target_node": node,
        "success": success,
        "violation": violation,
        "latency_ms": round(latency, 3),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "error": error,
    })


# -------------------------------------------------------------------
# Experiment
# -------------------------------------------------------------------

def main():

    consistency_level = CONSISTENCY_LEVELS[READ_CL_NAME]

    # Docker port mapping:
    #
    # node1 -> localhost:10001
    # node2 -> localhost:10002
    # node3 -> localhost:10003

    cluster1, session1 = connect(10001)
    cluster2, session2 = connect(10002)
    cluster3, session3 = connect(10003)

    nodes = [
        ("node1", session1),
        ("node2", session2),
        ("node3", session3),
    ]

    previous_version = None

    with open(OUTPUT_FILE, "w", newline="") as file:

        writer = csv.DictWriter(
            file,
            fieldnames=FIELDS
        )

        writer.writeheader()

        for trial_id in range(1, TRIALS + 1):

            # Rotate between nodes.
            node_name, session = nodes[
                (trial_id - 1) % len(nodes)
            ]

            version, latency, success, error = read_product(
                session,
                consistency_level
            )

            violation = False

            if (
                success
                and previous_version is not None
                and version < previous_version
            ):
                violation = True

            log_result(
                writer,
                trial_id,
                node_name,
                version,
                success,
                violation,
                latency,
                error,
            )

            if success:
                previous_version = version

            print(
                f"Trial {trial_id:03d} | "
                f"{node_name} | "
                f"version={version} | "
                f"success={success} | "
                f"violation={violation}"
            )

            time.sleep(0.05)

    cluster1.shutdown()
    cluster2.shutdown()
    cluster3.shutdown()


if __name__ == "__main__":
    main()