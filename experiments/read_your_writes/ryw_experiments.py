import subprocess
import sys
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent

# ========= Experiment Settings =========
SCENARIOS = ["normal", "node_failure", "partition"]
ITERATIONS = 100      # every CL pair run <iterations> times
REPEATS = 3           # each case repeats 3 times

# ======================================

for scenario in SCENARIOS:
    print(f"\n{'='*60}")
    print(f"Scenario: {scenario}")
    print(f"{'='*60}")

    for run in range(1, REPEATS + 1):
        print(f"\nRun {run}/{REPEATS}")

        cmd = [
            sys.executable,
            str(SCRIPT_DIR / "read_your_writes.py"),
            "--scenario", scenario,
            "--iterations", str(ITERATIONS)
        ]

        result = subprocess.run(cmd)

        if result.returncode != 0:
            print(f"Experiment failed: {scenario} Run {run}")
            break

        # give Cassandra some time to recover
        time.sleep(5)

print("\nAll experiments finished!")