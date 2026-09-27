# Cassandra Read-Your-Writes Experiments

## Overview

This project tests whether a logical client can observe its own acknowledged writes in a three-node Apache Cassandra cluster. It compares six write/read consistency configurations under normal operation, single-node failure, and network partition.

The experiment uses a fixed number of independent trials. Each trial uses a new UUID key initialized to version `0` on all replicas. Client A writes version `1` and, only if the write succeeds, immediately reads the same key. Initialization and measurement use explicitly increasing timestamps.

## Project Files

| File | Purpose |
|---|---|
| `experiments/read_your_writes/read_your_writes.py` | Runs experiments, injects faults, records operations, and restores the cluster. |
| `experiments/read_your_writes/ryw_experiments.py` | Runs all three scenarios sequentially, with 100 trials per configuration and three repetitions per scenario. |
| `experiments/summarize_ryw.py` | Combines eligible experiment summaries into one CSV table. |
| `docker-compose.yml` | Defines the three-node Cassandra deployment. |
| `Dockerfile.cassandra` | Installs iptables on node3 for network-partition experiments. |
| `environment.yml` | Defines the Python environment and dependencies. |
| `results/read_your_writes/` | Contains operation logs, per-run summaries, and metadata. |

## 1. Environment and Installation

Run the commands below in PowerShell from the project root. Start Docker Desktop with Linux containers enabled.

The deployment uses Cassandra 5.0.9. The Python environment specifies Python 3.12, cassandra-driver 3.30.1, and pyasyncore 1.0.5. The three nodes belong to `datacenter1`. The experiment uses the `ryw_matrix.samples` table with `NetworkTopologyStrategy` and replication factor `RF=3`.

| Container | Host CQL port | Role in the experiments |
|---|---:|---|
| `cassandra-1` | 10001 | Write coordinator |
| `cassandra-2` | 10002 | Read coordinator in normal and node-failure scenarios |
| `cassandra-3` | 10003 | Additional normal read coordinator; stopped or isolated in fault scenarios |

The container CQL port is 9042. A coordinator handles the client request; it is not necessarily the only replica participating in that operation.

### Start the cluster

```powershell
docker compose up -d --build
docker compose ps
docker exec cassandra-1 nodetool status
```

Wait until all containers are healthy and all three nodes are `UN` (Up/Normal). The experiment also checks the cluster before sampling. Building the deployment ensures that node3 has iptables; existing named data volumes are reused.

Check node3's firewall access without changing any rules:

```powershell
docker exec --user root cassandra-3 iptables -S
```

### Set up Python

If the project already has a working `.venv`, use the `.venv/Scripts/python.exe` commands below.

Alternatively, create and activate the supplied Conda environment:

```powershell
conda env create -f environment.yml
conda activate DSA5208_Project1_CMDD
```

To update an existing environment:

```powershell
conda env update -n DSA5208_Project1_CMDD -f environment.yml
```

For individual experiments and CSV aggregation in Conda, replace `.venv/Scripts/python.exe` with `python`. The batch runner uses the same Python interpreter that was used to launch it.

`pyasyncore` supplies compatibility support required by the driver on Python 3.12. Docker, Cassandra, and iptables are provided separately through the Docker deployment.

## 2. Experimental Design

All configurations are written in **write/read order**:

| Write CL | Read CL | Required write responses + read responses | Replica intersection guaranteed under the stated assumptions? |
|---|---|---:|---|
| ONE | ONE | 2 | No |
| ONE | QUORUM | 3 | No |
| QUORUM | ONE | 3 | No |
| QUORUM | QUORUM | 4 | Yes |
| ALL | ONE | 4 | Yes |
| ONE | ALL | 4 | Yes |

With RF=3, ONE, QUORUM, and ALL require responses from one, two, and three replicas respectively. Under this experiment's single-writer, fixed-topology, increasing-timestamp conditions, configurations with `W + R > RF` are expected to satisfy RYW when both operations succeed. Configurations without guaranteed intersection may still show no violations in a finite experiment.

All keys are initialized at ALL before fault injection. For fault scenarios, sampling begins only after the expected fault topology is confirmed.

| Scenario | Fault setup | Write coordinator | Read coordinator |
|---|---|---|---|
| `normal` | All nodes remain available. | node1 | Alternates between node2 and node3 |
| `node_failure` | Stop node3 and confirm it is down before sampling. | node1 | node2 |
| `partition` | Isolate node3 from internode communication while retaining client access. | node1 | node3 |

The partition blocks node3's TCP ports 7000/7001 using the dedicated `CMDD_RYW` iptables chain. CQL access remains available. All connections are controlled sequentially by the same logical client A.

In the partition scenario, ONE/ONE and QUORUM/ONE are expected to expose stale reads through isolated node3. Reads requiring QUORUM or ALL through that node are expected to fail, and writes requiring ALL are expected to fail. These failures are availability outcomes, not successful stale observations.

## 3. Run the Experiments

Run experiments sequentially. Other group members should not use the cluster while fault experiments are running.

### Quick check

```powershell
.venv/Scripts/python.exe experiments/read_your_writes/read_your_writes.py --iterations 3
```

Without arguments, the experiment runs the normal scenario with 30 trials per configuration. Both defaults can be overridden.

### One scenario, all six configurations

```powershell
.venv/Scripts/python.exe experiments/read_your_writes/read_your_writes.py --scenario normal --iterations 100
.venv/Scripts/python.exe experiments/read_your_writes/read_your_writes.py --scenario node_failure --iterations 100
.venv/Scripts/python.exe experiments/read_your_writes/read_your_writes.py --scenario partition --iterations 100
```

### One configuration

Specify both consistency levels together:

```powershell
.venv/Scripts/python.exe experiments/read_your_writes/read_your_writes.py --scenario normal --iterations 100 --write-cl QUORUM --read-cl QUORUM
```

### Full batch

```powershell
.venv/Scripts/python.exe experiments/read_your_writes/ryw_experiments.py
```

The current batch settings are `ITERATIONS = 100` and `REPEATS = 3`. A fully completed batch produces nine runs and 5,400 trials:

`3 scenarios x 6 configurations x 3 repetitions x 100 trials`.

The script waits five seconds after each successful run. If a run exits with an error, it skips the remaining repetitions of that scenario and proceeds to the next scenario. The final `All experiments finished!` message does not establish that every run succeeded: check each run's metadata and recovery status.

## 4. Output Files and Interpretation

Each run creates a separate `results/read_your_writes/<scenario>_<UUID>/` directory containing:

| File | Contents |
|---|---|
| `trials.csv` | One row per attempted measured WRITE or READ operation. Initialization operations are excluded. |
| `summary.csv` | Counts and violation rates for each consistency configuration in that run. |
| `metadata.json` | Run parameters, software versions, deployment configuration, status, fault evidence, and recovery results. |

A write and its subsequent read share the same `trial_id`. If the write fails, the read is skipped and only the write row is recorded. Consequently, the number of operation rows is not the number of trials.

### Operation fields

| Field | Meaning |
|---|---|
| `trial_id` | Run UUID, write/read configuration, and trial index. |
| `model` | `RYW`. |
| `scenario` | `normal`, `node_failure`, or `partition`. |
| `client` | Logical client `A`. |
| `operation` | `WRITE` or `READ`. |
| `key` | The trial's UUID key. |
| `version_written` | Expected written version, currently `1`. |
| `version_observed` | Returned version; blank for writes, failed reads, or missing records. |
| `read_cl`, `write_cl` | Requested consistency levels. |
| `target_node` | Coordinator contacted for this operation. |
| `success` | Whether the request completed successfully. A stale or empty result can still be a successful request. |
| `violation` | Whether a successful read violates RYW; blank for writes and failed reads. |
| `latency_ms` | Client-observed request latency, including failed requests. |
| `timestamp` | Operation start time in ISO 8601 format with timezone. |
| `error` | Exception type and message for a failed operation. |

### Outcome definitions

| Outcome | Definition |
|---|---|
| PASS | An acknowledged write is followed by a successful read of version 1 or newer. |
| VIOLATION | An acknowledged write is followed by a successful read of an older version or a missing record. |
| WRITE_FAILED | The write is not acknowledged; the subsequent read is skipped. |
| READ_FAILED | The write is acknowledged, but the subsequent read fails. |

These categories explain the results; there is no separate `outcome` column in the operation CSV.

The violation rate is `violations / valid_reads`, where valid reads include both passes and violations. If no valid reads exist, the per-run rate is blank and the aggregated table displays `N/A`, not 0%. A write timeout does not prove that the write had no effect.

Use completed runs in the report. Fault runs must also have verified recovery. Exit code 0 indicates that the experiment procedure completed, not that every database request succeeded or that there were no violations.

The fixed-trial format uses `schema_version=2` and `row_grain=operation`. Do not combine it with continuous-workload results or older incompatible formats.

## 5. Generate the Combined CSV Table

After collecting complete runs for all three scenarios:

```powershell
.venv/Scripts/python.exe experiments/read_your_writes/summarize_ryw.py
```

The script writes only one aggregated table:

```text
results/read_your_writes/aggregated/ryw_complete_table.csv
```

It sums counts across eligible runs and then calculates violation rates. It does not average per-run percentages. The table contains 18 rows: three scenarios times six configurations.

The aggregator reads run directories directly beneath `results/read_your_writes/`, requires all six configurations in each eligible run, rejects duplicate run IDs and inconsistent counts, and skips incomplete runs or fault runs without verified recovery. Each scenario must have at least one eligible run. Keep single-configuration checks separate from the collection used for the final table.

All eligible runs in the selected input directory are included, including additional repetitions collected later. To select another collection and output directory:

```powershell
.venv/Scripts/python.exe experiments/read_your_writes/summarize_ryw.py --results-dir path/to/runs --output-dir path/to/output
```

Close the output CSV in Excel or WPS before regenerating it if the application locks the file.

## 6. Recovery

Fault experiments attempt to restore the cluster on completion or an exception. If the process is forcibly terminated, run:

```powershell
.venv/Scripts/python.exe experiments/read_your_writes/read_your_writes.py --recover
```

This starts node3, removes only this experiment's `CMDD_RYW` rules, and verifies that all three nodes are UN and reachable through CQL. It does not remove firewall rules belonging to other experiments.

## 7. Limitations and Validation

The experiment uses Docker containers on one physical machine, one logical client, independent keys, fixed coordinator choices, and increasing timestamps. It does not evaluate concurrent writers, clock anomalies, TTLs, deletions, or all possible failure layouts.

The node-failure scenario reads through a surviving node, whereas the partition scenario reads through the isolated node. Interpret differences with both fault type and coordinator placement in mind. Zero observed violations do not establish a universal guarantee, and operation failures must be reported separately from RYW violations.

The existing fixed-trial dataset was previously checked as nine completed runs totaling 5,400 trials, with fault recovery recorded as verified. Updating this README does not rerun the database experiments.

To check dependency imports and command-line options without starting experiments:

```powershell
.venv/Scripts/python.exe -c "import cassandra, asyncore; print(cassandra.__version__)"
.venv/Scripts/python.exe experiments/read_your_writes/read_your_writes.py --help
.venv/Scripts/python.exe experiments/read_your_writes/summarize_ryw.py --help
```
