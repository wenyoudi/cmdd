#!/bin/bash

set -e

# ============================================================
# Automated Monotonic Reads - Normal Operation Experiments
# ============================================================

TRIALS=3
DURATION=60

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

RUN_PY="$SCRIPT_DIR/run.py"
RESULT_DIR="$PROJECT_ROOT/results"

mkdir -p "$RESULT_DIR"

SUMMARY_FILE=$(mktemp)


# ------------------------------------------------------------
# Safety cleanup
# ------------------------------------------------------------

cleanup() {
    rm -f "$SUMMARY_FILE"
}

trap cleanup EXIT INT TERM


# ------------------------------------------------------------
# Summary header
# ------------------------------------------------------------

echo \
"READ_CL,WRITE_CL,TRIAL,TOTAL_READS,SUCCESSFUL_READS,FAILED_READS,VIOLATIONS,VIOLATION_RATE" \
> "$SUMMARY_FILE"


# ------------------------------------------------------------
# Consistency configurations
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

# ============================================================
# Experiment information
# ============================================================

echo "============================================================"
echo "MONOTONIC READS - NORMAL OPERATION TRIALS"
echo "============================================================"
echo
echo "Trials per configuration: $TRIALS"
echo "Reader duration:          ${DURATION}s"
echo "Configurations:           ${#CONFIGS[@]}"
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
        # 1. Verify all Cassandra nodes are Up/Normal
        # ----------------------------------------------------

        echo "Checking Cassandra cluster..."

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
        # 2. Build output filename
        # ----------------------------------------------------

        READ_LOWER=$(
            echo "$READ_CL" \
            | tr '[:upper:]' '[:lower:]'
        )

        WRITE_LOWER=$(
            echo "$WRITE_CL" \
            | tr '[:upper:]' '[:lower:]'
        )

        OUTPUT="$RESULT_DIR/mr_normal_${READ_LOWER}_${WRITE_LOWER}_trial${trial}.csv"

        echo "Output: $OUTPUT"


        # ----------------------------------------------------
        # 3. Run experiment
        # ----------------------------------------------------

        echo
        echo "Starting normal-operation workload..."

        python "$RUN_PY" \
            --scenario normal \
            --read-cl "$READ_CL" \
            --write-cl "$WRITE_CL" \
            --duration "$DURATION" \
            --countdown 0 \
            --output "$OUTPUT"


        # ----------------------------------------------------
        # 4. Extract trial summary from CSV
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

        echo \
"$READ_CL,$WRITE_CL,$trial,$SUMMARY" \
>> "$SUMMARY_FILE"


        # ----------------------------------------------------
        # 5. Small pause between trials
        # ----------------------------------------------------

        echo
        echo "Trial complete."

        if [ "$trial" -lt "$TRIALS" ]; then
            echo "Waiting 5s before next trial..."
            sleep 5
        fi

    done
done


# ============================================================
# Final summary
# ============================================================

echo
echo "========================================================================================================="
echo "FINAL NORMAL-OPERATION SUMMARY"
echo "========================================================================================================="
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
echo "========================================================================================================="
echo "ALL NORMAL-OPERATION TRIALS COMPLETE"
echo "========================================================================================================="