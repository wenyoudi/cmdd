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

Run all commands from the repository root. If no individual consistency levels are specified, the script executes all six `(W1, W2)` configurations.

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

## Limitation

The experiment assigns explicitly increasing Cassandra timestamps to the baseline, `W1`, and `W2`. Cassandra resolves conflicting values using timestamp-based last-write-wins semantics, which can help preserve the intended `W1 -> W2` order in this setup.

Consequently, the absence of an observed anomaly must not be interpreted as a general guarantee that Cassandra provides Monotonic Writes under all executions. The results describe only the tested configurations, fault scenarios, and experimental criterion.
