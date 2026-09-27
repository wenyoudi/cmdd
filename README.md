# DSA5208 Project 1 – Client-Centric Consistency Experiments

This repository contains experiments evaluating four client-centric consistency models in a three-node Apache Cassandra cluster:

- **Monotonic Reads (MR)**
- **Monotonic Writes (MW)**
- **Read Your Writes (RYW)**
- **Writes Follow Reads (WFR)**

The experiments evaluate these consistency properties under three scenarios:

1. Normal operation
2. Single-node failure
3. Network partition

Different Cassandra consistency-level configurations (`ONE`, `QUORUM`, and `ALL`) are tested to study consistency behavior and availability under these scenarios.

## 1. Environment Setup

The experiments use a three-node Apache Cassandra cluster deployed with Docker Compose.

From the project root, start the cluster:

```bash
docker compose up -d --build
```

Verify that all three Cassandra nodes are running:

```bash
docker exec cassandra-1 nodetool status
```

Wait until all three nodes report `UN` (Up/Normal) before running the experiments.

The Python environment is specified in `environment.yml`:

```bash
conda env create -f environment.yml
conda activate DSA5208_Project1_CMDD
```

All experiment commands below should be run from the project root.

## 2. Experiments

### 2.1 Monotonic Reads (MR)

The Monotonic Reads experiment checks whether a logical reader ever observes a version older than one it has previously observed.

Run the three scenarios:

```bash
# Normal operation
./experiments/monotonic_reads/run_baseline.sh

# Network partition
./experiments/monotonic_reads/run_partition.sh

# Node failure and recovery
./experiments/monotonic_reads/run_node_failure.sh
```

After generating the results, run the analysis:

```bash
python experiments/monotonic_reads/analysis.py
```

Detailed methodology and reproduction instructions:

[`experiments/monotonic_reads/README.md`](experiments/monotonic_reads/README.md)

---

### 2.2 Monotonic Writes (MW)

The Monotonic Writes experiment tests whether two writes issued sequentially by the same logical client are observed in the intended order.

Each scenario should be independently executed three times.

```bash
# Normal operation
python experiments/monotonic_writes/run.py \
  --scenario normal \
  --iterations 10

# Node failure
python experiments/monotonic_writes/run.py \
  --scenario node_failure \
  --iterations 10

# Network partition
python experiments/monotonic_writes/run.py \
  --scenario partition \
  --iterations 10
```

Each execution automatically tests all six configured write-consistency-level combinations.

Detailed methodology and reproduction instructions:

[`experiments/monotonic_writes/README.md`](experiments/monotonic_writes/README.md)

---

### 2.3 Read Your Writes (RYW)

The Read Your Writes experiment tests whether a logical client can observe its own acknowledged write when subsequently reading the same key.

The complete experiment batch can be run with:

```bash
python experiments/read_your_writes/ryw_experiments.py
```

The batch runs:

- normal operation;
- node failure; and
- network partition;

using six write/read consistency configurations, 100 trials per configuration, and three independent repetitions per scenario.

After the experiments complete, generate the aggregated result table with:

```bash
python experiments/read_your_writes/summarize_ryw.py
```

Detailed methodology, individual scenario commands, and recovery instructions:

[`experiments/read_your_writes/README.md`](experiments/read_your_writes/README.md)

---

### 2.4 Writes Follow Reads (WFR)

The Writes Follow Reads experiment tests whether a write that causally follows a read remains ordered after the version observed by that read.

Each scenario should be independently executed three times.

```bash
# Normal operation
python experiments/writes_follow_reads/run.py \
  --scenario normal \
  --iterations 10

# Node failure
python experiments/writes_follow_reads/run.py \
  --scenario node_failure \
  --iterations 10

# Network partition
python experiments/writes_follow_reads/run.py \
  --scenario partition \
  --iterations 10
```

Each execution automatically tests all six configured read/write consistency-level combinations.

Detailed methodology and reproduction instructions:

[`experiments/writes_follow_reads/README.md`](experiments/writes_follow_reads/README.md)

## 3. Fault Scenarios

In the **node-failure** experiments, `cassandra-3` is stopped while the experiment runs. The experiment scripts restore the node after the fault scenario.

In the **network-partition** experiments, Cassandra internode communication between `cassandra-3` and the other replicas is blocked while its CQL interface remains accessible. This allows experiments to observe behavior on an isolated replica.

The experiment-specific READMEs contain the exact fault-injection, recovery, and validation procedures.

## 4. Results

Results are organized by consistency model:

```text
results/
├── monotonic_reads/
├── monotonic_writes/
├── read_your_writes/
└── writes_follow_reads/
```

The experiments record successful operations, consistency violations or possible anomalies, operation failures, latency, consistency levels, target nodes, and other experiment-specific metadata.

Operation failures caused by unavailable consistency levels are treated separately from consistency violations or anomalies.

Refer to each experiment's README for the exact output schema and interpretation criteria.

## 5. Repository Structure

```text
.
├── docker-compose.yml
├── Dockerfile.cassandra
├── environment.yml
│
├── experiments/
│   ├── monotonic_reads/
│   ├── monotonic_writes/
│   ├── read_your_writes/
│   └── writes_follow_reads/
│
└── results/
    ├── monotonic_reads/
    ├── monotonic_writes/
    ├── read_your_writes/
    └── writes_follow_reads/
```

Each directory under `experiments/` contains a dedicated `README.md` with the detailed experimental design, consistency configurations, execution instructions, output interpretation, and limitations for that consistency model.