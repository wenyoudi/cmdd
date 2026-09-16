#!/bin/bash

set -e

# ============================================================
# Automated Monotonic Reads Network-Partition Experiments
# ============================================================

TRIALS=3
DURATION=60

# Timing relative to the ACTUAL reader workload start.
#
# 0-10s  : healthy
# 10-35s : network partition
# 35-60s : healed
PARTITION_AT=10
HEAL_AT=35

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

RUN_PY="$SCRIPT_DIR/run.py"
PARTITION_SCRIPT="$SCRIPT_DIR/partition_node3.sh"
HEAL_SCRIPT="$SCRIPT_DIR/heal_node3.sh"

RESULT_DIR="$PROJECT_ROOT/results"

mkdir -p "$RESULT_DIR"

SUMMARY_FILE=$(mktemp)

echo "READ_CL,WRITE_CL,TRIAL,TOTAL_READS,SUCCESSFUL_READS,FAILED_READS,VIOLATIONS,VIOLATION_RATE" \
    > "$SUMMARY_FILE"


# ------------------------------------------------------------
# Consistency-level configurations
#
# Format:
#   READ_CL WRITE_CL
# ------------------------------------------------------------

CONFIGS=(
    "ONE ONE"
    "ONE QUORUM"
    "QUORUM ONE"
    "QUORUM QUORUM"
    "ALL ONE"
    "ONE ALL"
)

# ------------------------------------------------------------
# Track the currently running experiment / ready file.
# Used by cleanup if the script is interrupted.
# ------------------------------------------------------------

EXPERIMENT_PID=""
READY_FILE=""


# ------------------------------------------------------------
# Safety cleanup
#
# If Ctrl+C is pressed or the script exits unexpectedly while
# Cassandra-3 is partitioned, attempt to heal the partition.
# ------------------------------------------------------------

cleanup() {
    echo
    echo "Cleaning up..."

    "$HEAL_SCRIPT" >/dev/null 2>&1 || true

    if [ -n "$READY_FILE" ]; then
        rm -f "$READY_FILE"
    fi

    rm -f "$SUMMARY_FILE"
}

trap cleanup EXIT INT TERM


# ------------------------------------------------------------
# Validate timing configuration
# ------------------------------------------------------------

if [ "$PARTITION_AT" -ge "$HEAL_AT" ]; then
    echo "ERROR: PARTITION_AT must be smaller than HEAL_AT."
    exit 1
fi

if [ "$HEAL_AT" -ge "$DURATION" ]; then
    echo "ERROR: HEAL_AT must be smaller than DURATION."
    exit 1
fi


# ------------------------------------------------------------
# Experiment information
# ------------------------------------------------------------

echo "============================================================"
echo "MONOTONIC READS - AUTOMATED NETWORK PARTITION TRIALS"
echo "============================================================"
echo
echo "Trials per configuration: $TRIALS"
echo "Reader duration:          ${DURATION}s"
echo "Partition at:             ${PARTITION_AT}s after reader start"
echo "Heal at:                  ${HEAL_AT}s after reader start"
echo "Partition duration:       $((HEAL_AT - PARTITION_AT))s"
echo


# ============================================================
# Run experiments
# ============================================================

for config in "${CONFIGS[@]}"; do

    read -r READ_CL WRITE_CL <<< "$config"

    for trial in $(seq 1 "$TRIALS"); do

        echo
        echo "============================================================"
        echo "Configuration: READ=$READ_CL WRITE=$WRITE_CL"
        echo "Trial:         $trial/$TRIALS"
        echo "============================================================"


        # ----------------------------------------------------
        # 1. Ensure previous partition is gone
        # ----------------------------------------------------

        echo "Ensuring Cassandra-3 is not partitioned..."

        "$HEAL_SCRIPT" >/dev/null 2>&1 || true

        echo "Waiting for cluster to settle..."
        sleep 10


        # ----------------------------------------------------
        # 2. Verify all three Cassandra nodes are Up/Normal
        # ----------------------------------------------------

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
            echo "Aborting experiments."
            exit 1
        fi

        echo "Cluster healthy: 3/3 nodes UN."


        # ----------------------------------------------------
        # 3. Build output filename
        # ----------------------------------------------------

        READ_LOWER=$(
            echo "$READ_CL" \
            | tr '[:upper:]' '[:lower:]'
        )

        WRITE_LOWER=$(
            echo "$WRITE_CL" \
            | tr '[:upper:]' '[:lower:]'
        )

        OUTPUT="$RESULT_DIR/mr_network_partition_${READ_LOWER}_${WRITE_LOWER}_trial${trial}.csv"

        echo "Output: $OUTPUT"


        # ----------------------------------------------------
        # 4. Create unique synchronization file path
        #
        # run.py will create this file immediately before the
        # measured reader workload begins.
        # ----------------------------------------------------

        READY_FILE="/tmp/mr_ready_${READ_LOWER}_${WRITE_LOWER}_${trial}_$$"

        rm -f "$READY_FILE"


        # ----------------------------------------------------
        # 5. Start Python workload in background
        # ----------------------------------------------------

        python "$RUN_PY" \
            --scenario network_partition \
            --read-cl "$READ_CL" \
            --write-cl "$WRITE_CL" \
            --duration "$DURATION" \
            --countdown 0 \
            --ready-file "$READY_FILE" \
            --output "$OUTPUT" &

        EXPERIMENT_PID=$!

        echo
        echo "Experiment PID: $EXPERIMENT_PID"
        echo "Python process started."
        echo "Waiting for reader workload to become ready..."


        # ----------------------------------------------------
        # 6. Wait for run.py to signal that the reader is
        #    actually about to start.
        #
        # This is the synchronization fix.
        # ----------------------------------------------------

        while [ ! -f "$READY_FILE" ]; do

            # Check whether Python died during initialization.
            if ! kill -0 "$EXPERIMENT_PID" 2>/dev/null; then
                echo
                echo "ERROR: Experiment exited before reader started."

                wait "$EXPERIMENT_PID" || true

                exit 1
            fi

            sleep 0.1
        done


        # ----------------------------------------------------
        # Time zero = actual reader workload start
        # ----------------------------------------------------

        START_TIME=$(date +%s)

        echo
        echo "Reader workload started."
        echo "Fault timer begins NOW."


        # ====================================================
        # PHASE 1: Healthy
        # ====================================================

        echo
        echo "[Phase 1] Healthy cluster"
        echo "Duration: ${PARTITION_AT}s"

        sleep "$PARTITION_AT"


        # ====================================================
        # PHASE 2: Network partition
        # ====================================================

        echo
        echo "[Phase 2] Applying network partition..."

        "$PARTITION_SCRIPT"

        PARTITION_DURATION=$((HEAL_AT - PARTITION_AT))

        echo
        echo "Partition active."
        echo "Duration: ${PARTITION_DURATION}s"

        sleep "$PARTITION_DURATION"


        # ====================================================
        # PHASE 3: Heal network partition
        # ====================================================

        echo
        echo "[Phase 3] Healing network partition..."

        "$HEAL_SCRIPT"

        echo
        echo "Partition healed."


        # ====================================================
        # PHASE 4: Remaining healthy workload
        # ====================================================

        echo
        echo "[Phase 4] Waiting for reader workload to finish..."

        wait "$EXPERIMENT_PID"

        # ----------------------------------------------------
        # Extract trial summary directly from the CSV
        # ----------------------------------------------------

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

        success = row["success"].strip().lower() == "true"
        violation = row["violation"].strip().lower() == "true"

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

        echo "$READ_CL,$WRITE_CL,$trial,$SUMMARY" >> "$SUMMARY_FILE"

        EXPERIMENT_PID=""


        # ----------------------------------------------------
        # 7. Remove synchronization file
        # ----------------------------------------------------

        rm -f "$READY_FILE"
        READY_FILE=""


        # ----------------------------------------------------
        # 8. Report elapsed reader time
        # ----------------------------------------------------

        END_TIME=$(date +%s)
        ELAPSED=$((END_TIME - START_TIME))

        echo
        echo "Trial completed."
        echo "Measured workload elapsed time: approximately ${ELAPSED}s"
        echo "Results: $OUTPUT"

    done
done


# ============================================================
# Complete
# ============================================================

echo
echo "=========================================================================================="
echo "FINAL EXPERIMENT SUMMARY"
echo "=========================================================================================="
echo

printf "%-10s %-10s %-7s %-12s %-12s %-10s %-12s %-15s\n" \
    "READ" \
    "WRITE" \
    "TRIAL" \
    "TOTAL_READS" \
    "SUCCESSFUL" \
    "FAILED" \
    "VIOLATIONS" \
    "VIOLATION_RATE"

printf "%-10s %-10s %-7s %-12s %-12s %-10s %-12s %-15s\n" \
    "----------" \
    "----------" \
    "-------" \
    "------------" \
    "------------" \
    "----------" \
    "------------" \
    "---------------"

tail -n +2 "$SUMMARY_FILE" |
while IFS=',' read -r \
    read_cl \
    write_cl \
    trial \
    total \
    successful \
    failed \
    violations \
    rate
do

    printf "%-10s %-10s %-7s %-12s %-12s %-10s %-12s %-15s\n" \
        "$read_cl" \
        "$write_cl" \
        "$trial" \
        "$total" \
        "$successful" \
        "$failed" \
        "$violations" \
        "$rate"

done

echo
echo "=========================================================================================="

rm -f "$SUMMARY_FILE"

echo
echo "ALL NETWORK PARTITION TRIALS COMPLETE"
echo "============================================================"