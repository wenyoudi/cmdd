# Monotonic Reads Experiment

This directory contains the scripts used to test **monotonic-reads consistency** on the three-node Apache Cassandra cluster.

## Experiment Design

Each trial uses a fresh UUID-based key of the form:

```text
mr-<trial_id>
```

The key is initialized with `version = 0`. A writer then continuously updates the same key with monotonically increasing version numbers (`1, 2, 3, ...`), while a logical reader repeatedly reads the key through different Cassandra nodes.

A monotonic-read violation is recorded when a successful read returns a version lower than the highest version previously observed by the same logical reader:

```text
version_observed < highest_version_previously_seen
```

Each operation is logged to a CSV file in `results/`.

## Consistency Configurations

The normal-operation and network-partition experiments test the following read/write consistency-level combinations:

```text
READ ONE     / WRITE ONE
READ ONE     / WRITE QUORUM
READ QUORUM  / WRITE ONE
READ QUORUM  / WRITE QUORUM
READ ALL     / WRITE ONE
READ ONE     / WRITE ALL
```

Each configuration is repeated for three trials.

The node-failure/recovery experiment uses `READ ONE / WRITE ONE` for three trials to examine availability during node failure and whether a recovered replica causes a monotonic-read regression.

## Prerequisites

Start the three-node Cassandra cluster from the project root using the project's Docker Compose configuration.

Verify that all three nodes are `UN` (Up/Normal):

```bash
docker exec cassandra-1 nodetool status
```

The experiments expect the following containers:

```text
cassandra-1
cassandra-2
cassandra-3
```

The Python environment must contain the Cassandra Python driver and the other dependencies specified by the project environment.

For the network-partition experiment, `cassandra-3` must have `NET_ADMIN` capability and `iptables` available, as configured by the project's `docker-compose.yml` and `Dockerfile.cassandra`.

## Running the Experiments

Run all commands from the project root.

### 1. Normal Operation

```bash
./experiments/monotonic_reads/run_baseline.sh
```

This runs all six consistency configurations for three trials each while all three Cassandra nodes remain healthy.

### 2. Network Partition

```bash
./experiments/monotonic_reads/run_partition.sh
```

Each 60-second trial follows this schedule:

```text
0–10 s   : normal operation
10–35 s  : cassandra-3 isolated from Cassandra internode communication
35–60 s  : partition healed / recovery
```

The partition blocks Cassandra internode traffic on ports `7000` and `7001` for `cassandra-3`, while leaving CQL port `9042` accessible. This allows the client to continue querying the isolated replica while it may become stale.

The partition is created and removed automatically using:

```text
partition_node3.sh
heal_node3.sh
```

The workload and fault timing are synchronized so that the partition timer begins only after the measured reader workload has started.

If necessary, the partition can also be healed manually with:

```bash
./experiments/monotonic_reads/heal_node3.sh
```

### 3. Node Failure and Recovery

```bash
./experiments/monotonic_reads/run_node_failure.sh
```

This experiment runs three `READ ONE / WRITE ONE` trials.

Each 60-second trial follows:

```text
0–10 s   : all nodes healthy
10–30 s  : cassandra-3 stopped
30–60 s  : cassandra-3 restarted / recovery
```

The script automatically stops and restarts `cassandra-3` and waits for all three nodes to return to `UN` before beginning the next trial.

## Output

Experiment results are written to the project-level `results/` directory.

Examples:

```text
results/mr_normal_one_one_trial1.csv
results/mr_network_partition_one_one_trial1.csv
results/mr_network_partition_all_one_trial1.csv
results/mr_node_failure_one_one_trial1.csv
```

Each logged operation follows the common output schema:

```text
trial_id
model
scenario
client
operation
key
version_written
version_observed
read_cl
write_cl
target_node
success
violation
latency_ms
timestamp
error
```

Failed operations are recorded separately from monotonic-read violations. A monotonic-read violation therefore represents a **successful read that returned an older version**, rather than a read that simply failed.

## Analysis

After generating the result CSVs, run:

```bash
python experiments/monotonic_reads/analysis.py
```

The analysis reports:

- trial-level results;
- aggregated results by consistency configuration;
- read and write failure rates;
- monotonic-read violation rates;
- median and p95 read/write latency;
- violations by target Cassandra node;
- node-failure recovery behavior; and
- a cross-scenario comparison under `READ ONE / WRITE ONE`.

## Files

```text
monotonic_reads/
├── run.py                 # Main workload and MR violation detection
├── run_baseline.sh        # Automated normal-operation trials
├── run_partition.sh       # Automated network-partition trials
├── run_node_failure.sh    # Automated node-failure/recovery trials
├── partition_node3.sh     # Isolates cassandra-3 internode traffic
├── heal_node3.sh          # Restores cassandra-3 internode traffic
├── analysis.py            # Aggregates and analyzes experiment results
└── README.md              # Reproduction instructions
```

## Notes

- Each trial uses a fresh UUID-based key to prevent data from previous trials from affecting subsequent trials.
- `cassandra-3` is the deliberately faulted/isolated node in both abnormal scenarios.
- The scripts verify cluster health before starting trials where appropriate.
- Result CSVs may be overwritten if the same experiment is rerun with the same configuration and trial number.