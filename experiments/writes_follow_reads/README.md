# Writes-Follow-Reads (WFR) Experiment

## 1. Objective

This experiment investigates the Writes-Follow-Reads (WFR) client-centric consistency model in Apache Cassandra under three scenarios:

1. Normal operation
2. Single-node failure
3. Network partition

The experiment evaluates how different combinations of read and write consistency levels affect the visibility of causal dependencies, availability, and possible WFR anomalies.

---

## 2. Writes-Follow-Reads

Writes-Follow-Reads requires a write issued by a client after a read to be ordered after the version observed by that read.

The experiment represents this causal dependency using two values:

- `parent`: the antecedent value
- `child`: the dependent value

The initial state is:

```text
parent = 0
child  = 0
```

Client B first writes:

```text
WRITE parent = 1
```

Client A then reads `parent`.

Only when Client A successfully observes:

```text
parent = 1
```

is the dependency considered established. Client A then performs the dependent write:

```text
WRITE child = 1
```

Therefore, the causal sequence tested is:

```text
Client B: WRITE parent = 1
              ↓
Client A: READ parent = 1
              ↓
Client A: WRITE child = 1
```

The antecedent `parent` write is always performed at `QUORUM`. This keeps the creation of the antecedent fixed while the experiment varies the consistency levels of Client A's dependency read and dependent write.

---

## 3. Experimental Anomaly Criterion

After the dependency has been established and the dependent write has succeeded, the experiment verifies the state visible through each relevant Cassandra node.

A possible WFR anomaly is recorded when the same verification observation sees:

```text
parent = 0
child  = 1
```

This means that the dependent effect (`child = 1`) is visible while its antecedent (`parent = 1`) is not visible at that observation point.

The experiment therefore records a possible WFR anomaly only when all of the following conditions hold:

1. Client A successfully reads `parent = 1`.
2. The dependency is therefore established.
3. Client A's dependent write `child = 1` succeeds.
4. A verification observation returns `parent = 0` and `child = 1`.

Other states are interpreted differently:

```text
parent = 1, child = 1
```

Both the antecedent and dependent write are visible.

```text
parent = 1, child = 0
```

The antecedent is visible but the dependent write is not visible at that replica.

```text
parent = 0, child = 0
```

The observer sees an older state.

Operation failures caused by unavailable consistency levels are treated as availability failures, not WFR anomalies.

The detected pattern should be interpreted as a possible WFR anomaly under this experimental criterion rather than as a general proof that Cassandra violates WFR in all configurations.

---

## 4. Cassandra Configuration

The experiment uses a three-node Cassandra cluster running with Docker Compose.

```text
cassandra-1 → localhost:10001
cassandra-2 → localhost:10002
cassandra-3 → localhost:10003
```

The keyspace uses:

```text
Replication Factor = 3
Datacenter = datacenter1
```

Each trial uses a unique UUID key.

The experiment table stores:

```text
parent
child
```

for each trial.

---

## 5. Consistency-Level Matrix

The following combinations are tested:

| Dependency Read CL | Dependent Write CL |
|---|---|
| ONE | ONE |
| ONE | QUORUM |
| QUORUM | ONE |
| QUORUM | QUORUM |
| ALL | ONE |
| ONE | ALL |

The antecedent `parent = 1` write is fixed at `QUORUM`.

Each configuration uses 10 independent trials per repetition, and the formal experiment is independently repeated three times.

---

## 6. Experimental Scenarios

### 6.1 Normal Operation

All three Cassandra nodes are running and can communicate normally.

The dependent write is coordinated through `node2`.

Verification is performed through:

```text
node1
node2
node3
```

No network or node fault is introduced.

---

### 6.2 Node Failure

`cassandra-3` is stopped before the trials begin.

The remaining topology is:

```text
node1 ↔ node2

node3: DOWN
```

The parent write and dependency read are coordinated through `node1`.

The dependent write is coordinated through `node2`.

Verification is performed only through the surviving nodes:

```text
node1
node2
```

Operations requiring `ALL` may become unavailable because only two of the three replicas remain reachable.

The stopped node is restored automatically after the experiment.

---

### 6.3 Network Partition

The experiment isolates `cassandra-3` from `cassandra-1` and `cassandra-2` by blocking Cassandra internode communication on ports 7000 and 7001.

Conceptually:

```text
node1 ↔ node2    ||    node3
```

The CQL interface of `node3` remains reachable by the client.

Therefore, `node3` remains alive and can accept client requests even though it cannot communicate with the other Cassandra replicas.

The antecedent write is performed at `QUORUM` through `node1`, allowing `node1` and `node2` to observe:

```text
parent = 1
```

Client A performs its dependency read through `node1`.

The dependent write is then coordinated through the isolated `node3`.

With dependent write CL `ONE`, the isolated node may accept:

```text
child = 1
```

while still observing:

```text
parent = 0
```

This creates the target observation:

```text
parent = 0
child  = 1
```

The network partition is removed automatically after the experiment.

---

## 7. Running the Experiment

Run commands from the repository root.

### Normal Operation

```bash
python experiments/writes_follow_reads/run.py \
  --scenario normal \
  --iterations 10
```

Each scenario is executed three times independently. Each execution uses --iterations 10 and produces a separate result directory with a unique run ID.

### Node Failure

```bash
python experiments/writes_follow_reads/run.py \
  --scenario node_failure \
  --iterations 10
```

### Network Partition

```bash
python experiments/writes_follow_reads/run.py \
  --scenario partition \
  --iterations 10
```

The script automatically executes all six read/write consistency-level combinations.

A single consistency-level pair can also be tested using:

```bash
python experiments/writes_follow_reads/run.py \
  --scenario normal \
  --iterations 1 \
  --read-cl ONE \
  --write-cl ONE
```

If recovery is required after an interrupted fault experiment, run:

```bash
python experiments/writes_follow_reads/run.py --recover
```

---

## 8. Output Files

Each execution creates a directory under:

```text
results/writes_follow_reads/<scenario>_<run_id>/
```

Each result directory contains:

```text
trials.csv
summary.csv
metadata.json
```

### trials.csv

Contains operation-level observations including:

- trial ID
- scenario
- client
- operation
- key
- written/observed version
- read consistency level
- write consistency level
- target node
- success/failure
- violation flag
- latency
- timestamp
- error

### summary.csv

Contains aggregated results for each read/write consistency-level pair.

Important fields include:

- `trials`
- `dependency_established`
- `completed_trials`
- `successful_verifications`
- `wfr_anomalies`
- `parent_write_failed`
- `dependency_read_failed`
- `dependency_not_established`
- `dependent_write_failed`
- `verify_failed`

### metadata.json

Records run-level metadata and experimental configuration.

---

## 9. Formal Experimental Results

### 9.1 Normal Operation

All six consistency-level combinations completed successfully.

Aggregated across the three repetitions, for every configuration:

```text
trials                   = 30
dependency_established   = 30
completed_trials         = 30
successful_verifications = 90
wfr_anomalies            = 0
```

No operation failures or possible WFR anomalies were observed.

Formal result directories:

```text
results/writes_follow_reads/normal_05119d90-368c-4d42-9ef5-a7671346720d
results/writes_follow_reads/normal_b3d6d95d-0ef4-43e4-9706-6ec88a924041
results/writes_follow_reads/normal_e61e74a8-2998-455c-808a-6b728d0db26e
```

---

### 9.2 Node Failure

For the following configurations:

```text
ONE / ONE
ONE / QUORUM
QUORUM / ONE
QUORUM / QUORUM
```

all 30 trials completed successfully.

Each configuration produced:

```text
completed_trials         = 30
successful_verifications = 60
wfr_anomalies            = 0
```

Only the two surviving nodes were used for verification.

For:

```text
ALL / ONE
```

all 30 dependency reads failed because `READ ALL` could not obtain responses from all three replicas while `node3` was down.

For:

```text
ONE / ALL
```

the dependency was established in all 30 trials, but all 30 dependent writes failed because `WRITE ALL` could not obtain acknowledgements from all replicas.

These are availability failures rather than WFR anomalies.

Formal result directories:

```text
results/writes_follow_reads/node_failure_7f830685-0364-4fd0-a77a-76c97b622daf
results/writes_follow_reads/node_failure_12b2ec53-76be-4d3d-af53-dff041596fa2
results/writes_follow_reads/node_failure_7043fe77-a877-4681-9caf-1d6fb186dcac
```

---

### 9.3 Network Partition

The network partition produced a clear distinction between weak dependent writes and stronger consistency levels.

For:

```text
ONE / ONE
```

all 30 trials completed and 30 possible WFR anomalies were observed.

For:

```text
QUORUM / ONE
```

all 30 trials also completed and 30 possible WFR anomalies were observed.

In these configurations, the isolated `node3` could accept the dependent write at CL `ONE` while still lacking the antecedent value.

The target observation was therefore produced:

```text
node1/node2: parent = 1, child = 0
node3:       parent = 0, child = 1
```

For:

```text
ONE / QUORUM
QUORUM / QUORUM
ONE / ALL
```

the dependency was established in all 30 trials, but all 30 dependent writes failed because isolated node3 could not obtain enough replica acknowledgements.

For:

```text
ALL / ONE
```

all 30 dependency reads failed because the partition prevented READ ALL from reaching all replicas.

Formal result directories:

```text
results/writes_follow_reads/partition_ae597f8e-f0e7-439f-bb7f-4760f40d4dd2
results/writes_follow_reads/partition_07e9ebb6-514c-4017-a6ff-fc85ff38a9d0
results/writes_follow_reads/partition_ed3d0459-006f-4c09-b11f-65790002555f
```

---

## 10. Interpretation

The experiments demonstrate different behavior under normal operation, node failure, and network partition.

Under normal operation, no possible WFR anomalies were observed for any tested consistency-level combination.

Under a single-node failure, weaker and quorum-level operations remained available on the two surviving replicas, while operations requiring `ALL` became unavailable. No possible WFR anomaly was observed among completed trials.

Under network partition, possible WFR anomalies were consistently observed when the dependent write used CL `ONE`. Increasing the dependency read from `ONE` to `QUORUM` did not eliminate the observed anomaly because the dependent write could still be accepted by the isolated node.

Using `QUORUM` or `ALL` for the dependent write prevented that write from completing on the isolated node. This should be interpreted as an availability trade-off rather than evidence that every such execution completed while preserving WFR.

Overall, the results illustrate that client-centric causal behavior depends not only on the consistency level of the preceding read, but also on whether the subsequent dependent write can become visible independently of the state observed by that read.

No observed anomaly should be interpreted as evidence from these experiments, not as a universal guarantee of WFR by Cassandra.