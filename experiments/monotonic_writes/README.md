# Monotonic Writes (MW) Experiment

This experiment evaluates Monotonic Writes (MW) behavior in a three-node Apache Cassandra cluster under multiple consistency-level configurations and failure scenarios.

## Consistency Model and Protocol

Monotonic Writes requires writes issued by the same logical client to be applied in the order in which they were issued. Each trial uses the following ordered sequence on the same key:

```text
baseline(x, 0) -> W1(x, 1) -> W2(x, 2)
```

`W2` is issued only after `W1` succeeds. `W1` is coordinated through node 1, while `W2` is coordinated through node 2. After both writes complete, verification reads inspect the versions visible through the nodes available in the selected scenario.

## Cassandra Configuration

- Apache Cassandra 5.0.9
- Three nodes
- Replication factor: 3
- Datacenter: `datacenter1`
- W1 coordinator: node 1
- W2 coordinator: node 2

## Consistency-Level Configurations

The experiment tests six `(W1, W2)` consistency-level pairs:

| W1 consistency | W2 consistency |
| --- | --- |
| `ONE` | `ONE` |
| `ONE` | `QUORUM` |
| `QUORUM` | `ONE` |
| `QUORUM` | `QUORUM` |
| `ALL` | `ONE` |
| `ONE` | `ALL` |

Each consistency-level configuration uses 10 independent trials per repetition, and the formal experiment is independently repeated three times.

## Scenarios

### Normal

All three Cassandra nodes are available. Verification reads are performed through nodes 1, 2, and 3.

### Node Failure

Node 3 is stopped before the experiment. Verification reads are performed only through the surviving nodes, node 1 and node 2.

### Network Partition

Node 3 remains reachable through CQL, but Cassandra internode traffic on ports `7000` and `7001` is blocked between node 3 and the other nodes. Verification reads are performed through nodes 1, 2, and 3 so that replica divergence can be observed while the partition is active.

## Observation Semantics

Each trial begins with baseline version `0` before executing `W1` and `W2`.

- `2`: the latest state produced by `W2`.
- `0`: a stale, pre-write state caused by replica divergence. This is **not** classified as an MW violation because the replica has observed neither write.
- `1`: a W1-only state observed after both writes have completed. Under this experiment's criterion, this is recorded as a **possible MW ordering anomaly**.

If an operation cannot satisfy its requested consistency level, the resulting failure is classified as an availability failure, not a consistency violation. A trial in which `W1` or `W2` fails cannot demonstrate the completed `W1 -> W2` sequence.

## Running the Experiment

Run all commands from the repository root. If no individual consistency levels are specified, the script executes all six `(W1, W2)` configurations. Each scenario is executed three times independently. Each execution uses `--iterations 10` and produces a separate result directory with a unique run ID.

Normal operation:

```bash
python experiments/monotonic_writes/run.py \
  --scenario normal \
  --iterations 10
```


Node failure:

```bash
python experiments/monotonic_writes/run.py \
  --scenario node_failure \
  --iterations 10
```


Network partition:

```bash
python experiments/monotonic_writes/run.py \
  --scenario partition \
  --iterations 10
```


To run one consistency-level pair, specify both write consistency levels. For example:

```bash
python experiments/monotonic_writes/run.py \
  --scenario normal \
  --iterations 10 \
  --write1-cl ONE \
  --write2-cl QUORUM
```

## Output

Each run creates a new run-specific directory under:

```text
results/monotonic_writes/
```

The directory contains:

- `trials.csv`: trial- and operation-level observations.
- `summary.csv`: aggregated results for each `(W1, W2)` consistency-level pair.
- `metadata.json`: experiment configuration and execution metadata.

## Formal Experimental Results

Results below are aggregated across three independent repetitions, with 10 trials per consistency-level configuration in each repetition.

### Normal Operation

All six consistency-level configurations completed successfully across the three repetitions. For each configuration, all 30 trials completed and produced 90 node-level verification observations, all of which returned version `2`. No stale version-`0` observations or possible version-`1` MW ordering anomalies were observed.

### Node Failure

For `ONE/ONE`, `ONE/QUORUM`, `QUORUM/ONE`, and `QUORUM/QUORUM`, all 30 trials completed successfully. Each configuration produced 60 verification observations through the two surviving nodes, all of which returned version `2`. No possible MW ordering anomalies were observed.

For `ALL/ONE`, `W1` failed in all 30 trials because consistency level `ALL` could not be satisfied while node 3 was unavailable. For `ONE/ALL`, `W2` failed in all 30 trials for the same reason. These are availability failures rather than MW anomalies.

### Network Partition

For `ONE/ONE`, `ONE/QUORUM`, `QUORUM/ONE`, and `QUORUM/QUORUM`, all 30 trials completed successfully. For each configuration, nodes 1 and 2 produced 60 version-`2` observations in total, while isolated node 3 produced 30 stale version-`0` observations. No version-`1` observation was recorded.

For `ALL/ONE`, `W1` failed in all 30 trials, while for `ONE/ALL`, `W2` failed in all 30 trials because the required consistency level could not be satisfied across the partition.


## Limitation

The experiment assigns explicitly increasing Cassandra timestamps to the baseline, `W1`, and `W2`. Cassandra resolves conflicting values using timestamp-based last-write-wins semantics, which can help preserve the intended `W1 -> W2` order in this setup.

Consequently, the absence of an observed anomaly must not be interpreted as a general guarantee that Cassandra provides Monotonic Writes under all executions. The results describe only the tested configurations, fault scenarios, and experimental criterion.
