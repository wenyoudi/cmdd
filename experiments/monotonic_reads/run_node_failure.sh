#!/bin/bash

set -e

# ============================================================
# Automated Monotonic Reads - Node Failure / Recovery
# ============================================================

TRIALS=3
DURATION=60

# Timing relative to actual reader workload start:
#
# 0-10s  : all nodes healthy
# 10-30s : cassandra-3 stopped
# 30-60s : cassandra-3 restarted / recovery
FAIL_AT=10
RECOVER_AT=30

READ_CL="ONE"
WRITE_CL="ONE"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

RUN_PY="$SCRIPT_DIR/run.py"
RESULT_DIR="$PROJECT_ROOT/results"

mkdir -p "$RESULT_DIR"

SUMMARY_FILE=$(mktemp)

echo \
"TRIAL,TOTAL_READS,SUCCESSFUL_READS,FAILED_READS,VIOLATIONS,VIOLATION_RATE" \
> "$SUMMARY_FILE"

EXPERIMENT_PID=""
READY_FILE=""


# ------------------------------------------------------------
# Safety cleanup
#
# If the script is interrupted while cassandra-3 is stopped,
# try to start it again.
# ------------------------------------------------------------

cleanup() {
    echo
    echo "Cleaning up..."

    docker start cassandra-3 >/dev/null 2>&1 || true

    if [ -n "$READY_FILE" ]; then
        rm -f "$READY_FILE"
    fi

    rm -f "$SUMMARY_FILE"
}

trap cleanup EXIT INT TERM


# ------------------------------------------------------------
# Validate timing
# ------------------------------------------------------------

if [ "$FAIL_AT" -ge "$RECOVER_AT" ]; then
    echo "ERROR: FAIL_AT must be smaller than RECOVER_AT."
    exit 1
fi

if [ "$RECOVER_AT" -ge "$DURATION" ]; then
    echo "ERROR: RECOVER_AT must be smaller than DURATION."
    exit 1
fi


# ============================================================
# Experiment information
# ============================================================

echo "============================================================"
echo "MONOTONIC READS - NODE FAILURE / RECOVERY"
echo "============================================================"
echo
echo "Trials:              $TRIALS"
echo "Read CL:             $READ_CL"
echo "Write CL:            $WRITE_CL"
echo "Reader duration:     ${DURATION}s"
echo "Fail cassandra-3 at: ${FAIL_AT}s"
echo "Recover at:          ${RECOVER_AT}s"
echo "Failure duration:    $((RECOVER_AT - FAIL_AT))s"
echo


# ============================================================
# Trials
# ============================================================

for trial in $(seq 1 "$TRIALS"); do

    echo
    echo "============================================================"
    echo "NODE FAILURE TRIAL $trial/$TRIALS"
    echo "============================================================"


    # --------------------------------------------------------
    # 1. Ensure cassandra-3 is running
    # --------------------------------------------------------

    echo "Ensuring cassandra-3 is running..."

    docker start cassandra-3 >/dev/null 2>&1 || true

    echo "Waiting for cluster to become healthy..."

    # Give Cassandra time to recover/rejoin before checking.
    sleep 15


    # --------------------------------------------------------
    # 2. Verify all 3 nodes are Up/Normal
    # --------------------------------------------------------

    UN_COUNT=$(
        docker exec cassandra-1 nodetool status \
        | grep -c '^UN' || true
    )

    if [ "$UN_COUNT" -ne 3 ]; then
        echo
        echo "ERROR: Expected 3 UN Cassandra nodes."
        echo
        docker exec cassandra-1 nodetool status
        echo
        echo "Aborting."
        exit 1
    fi

    echo "Cluster healthy: 3/3 nodes UN."


    # --------------------------------------------------------
    # 3. Output file
    # --------------------------------------------------------

    OUTPUT="$RESULT_DIR/mr_node_failure_one_one_trial${trial}.csv"

    echo "Output: $OUTPUT"


    # --------------------------------------------------------
    # 4. Synchronization file
    # --------------------------------------------------------

    READY_FILE="/tmp/mr_node_failure_ready_${trial}_$$"

    rm -f "$READY_FILE"


    # --------------------------------------------------------
    # 5. Start workload
    # --------------------------------------------------------

    python "$RUN_PY" \
        --scenario node_failure \
        --read-cl "$READ_CL" \
        --write-cl "$WRITE_CL" \
        --duration "$DURATION" \
        --countdown 0 \
        --ready-file "$READY_FILE" \
        --output "$OUTPUT" &

    EXPERIMENT_PID=$!

    echo
    echo "Experiment PID: $EXPERIMENT_PID"
    echo "Waiting for reader workload to start..."


    # --------------------------------------------------------
    # 6. Wait for run.py ready signal
    # --------------------------------------------------------

    while [ ! -f "$READY_FILE" ]; do

        if ! kill -0 "$EXPERIMENT_PID" 2>/dev/null; then
            echo
            echo "ERROR: Experiment exited before reader started."

            wait "$EXPERIMENT_PID" || true

            exit 1
        fi

        sleep 0.1
    done

    START_TIME=$(date +%s)

    echo
    echo "Reader workload started."
    echo "Failure timer begins NOW."


    # ========================================================
    # PHASE 1: Healthy
    # ========================================================

    echo
    echo "[Phase 1] Healthy cluster"
    echo "Duration: ${FAIL_AT}s"

    sleep "$FAIL_AT"


    # ========================================================
    # PHASE 2: Stop cassandra-3
    # ========================================================

    echo
    echo "[Phase 2] Stopping cassandra-3..."

    docker stop cassandra-3 >/dev/null

    echo "cassandra-3 stopped."

    FAILURE_DURATION=$((RECOVER_AT - FAIL_AT))

    echo "Failure duration: ${FAILURE_DURATION}s"

    sleep "$FAILURE_DURATION"


    # ========================================================
    # PHASE 3: Restart cassandra-3
    # ========================================================

    echo
    echo "[Phase 3] Restarting cassandra-3..."

    docker start cassandra-3 >/dev/null

    echo "cassandra-3 restart requested."
    echo "Reader continues while the node recovers."


    # ========================================================
    # PHASE 4: Recovery period
    # ========================================================

    echo
    echo "[Phase 4] Recovery period"
    echo "Waiting for reader workload to finish..."

    wait "$EXPERIMENT_PID"

    EXPERIMENT_PID=""


    # --------------------------------------------------------
    # 7. Extract summary from CSV
    # --------------------------------------------------------

    SUMMARY=$(
python - "$OUTPUT" <<'PY'
import csv
import sys

path = sys.argv[1]

total = 0
successful = 0
failed = 0
violations = 0

with open(path, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)

    for row in reader:
        if row["operation"] != "read":
            continue

        total += 1

        success = (
            row["success"]
            .strip()
            .lower()
            == "true"
        )

        violation = (
            row["violation"]
            .strip()
            .lower()
            == "true"
        )

        if success:
            successful += 1
        else:
            failed += 1

        if violation:
            violations += 1

rate = (
    violations / successful * 100
    if successful
    else 0
)

print(
    f"{total},{successful},{failed},"
    f"{violations},{rate:.4f}%"
)
PY
    )

    echo "$trial,$SUMMARY" >> "$SUMMARY_FILE"


    # --------------------------------------------------------
    # 8. Cleanup ready signal
    # --------------------------------------------------------

    rm -f "$READY_FILE"
    READY_FILE=""


    # --------------------------------------------------------
    # 9. Wait for Cassandra-3 to become healthy before
    #    beginning another trial.
    # --------------------------------------------------------

    END_TIME=$(date +%s)
    ELAPSED=$((END_TIME - START_TIME))

    echo
    echo "Trial completed."
    echo "Measured workload elapsed time: approximately ${ELAPSED}s"
    echo "Waiting for cassandra-3 to recover..."

    # Wait up to 120 seconds for all three nodes to become UN.
    RECOVERY_OK=false

    for attempt in $(seq 1 24); do

        UN_COUNT=$(
            docker exec cassandra-1 nodetool status \
            | grep -c '^UN' || true
        )

        if [ "$UN_COUNT" -eq 3 ]; then
            RECOVERY_OK=true
            break
        fi

        sleep 5
    done

    if [ "$RECOVERY_OK" != true ]; then
        echo
        echo "ERROR: Cluster did not recover to 3 UN nodes."
        docker exec cassandra-1 nodetool status
        exit 1
    fi

    echo "Cluster recovered: 3/3 nodes UN."

done


# ============================================================
# Final summary
# ============================================================

echo
echo "=========================================================================================="
echo "FINAL NODE-FAILURE SUMMARY"
echo "=========================================================================================="
echo

printf "%-7s %-12s %-12s %-10s %-12s %-15s\n" \
    "TRIAL" \
    "TOTAL_READS" \
    "SUCCESSFUL" \
    "FAILED" \
    "VIOLATIONS" \
    "VIOLATION_RATE"

printf "%-7s %-12s %-12s %-10s %-12s %-15s\n" \
    "-------" \
    "------------" \
    "------------" \
    "----------" \
    "------------" \
    "---------------"

tail -n +2 "$SUMMARY_FILE" |
while IFS=',' read -r \
    trial \
    total \
    successful \
    failed \
    violations \
    rate
do

    printf "%-7s %-12s %-12s %-10s %-12s %-15s\n" \
        "$trial" \
        "$total" \
        "$successful" \
        "$failed" \
        "$violations" \
        "$rate"

done

echo
echo "=========================================================================================="
echo "ALL NODE-FAILURE TRIALS COMPLETE"
echo "=========================================================================================="