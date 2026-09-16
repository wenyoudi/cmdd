import subprocess
import time

# ========= Experiment Settings =========
SCENARIOS = ["normal", "node_failure", "partition"]
ITERATIONS = 10      # 每个 CL pair 跑多少次
REPEATS = 1           # 每个 scenario 重复几轮

# ======================================

for scenario in SCENARIOS:
    print(f"\n{'='*60}")
    print(f"Scenario: {scenario}")
    print(f"{'='*60}")

    for run in range(1, REPEATS + 1):
        print(f"\nRun {run}/{REPEATS}")

        cmd = [
            ".venv/Scripts/python.exe",
            "read_your_writes.py",
            "--scenario", scenario,
            "--iterations", str(ITERATIONS)
        ]

        result = subprocess.run(cmd)

        if result.returncode != 0:
            print(f"Experiment failed: {scenario} Run {run}")
            break

        # 给 Cassandra 一点恢复时间（保险）
        time.sleep(5)

print("\nAll experiments finished!")