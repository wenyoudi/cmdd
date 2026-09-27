# DSA5208 Project 1 – Consistency Model Experiments

This repository contains experiments evaluating four client-centric consistency models using a three-node Apache Cassandra cluster:

- Monotonic Reads (MR)
- Monotonic Writes (MW)
- Read Your Writes (RYW)
- Writes Follow Reads (WFR)

## Setup

The experiments use a three-node Cassandra cluster deployed with Docker.

```bash
docker compose up -d --build
docker exec cassandra-1 nodetool status
```

Wait until all three nodes report `UN` (Up/Normal) before running the experiments.

Python dependencies are specified in `environment.yml`.

```bash
conda env create -f environment.yml
conda activate DSA5208_Project1_CMDD
```

## Experiments

Detailed reproduction instructions for each experiment are provided in the corresponding experiment directory:

- [Monotonic Reads](experiments/monotonic_reads/README.md)
- [Monotonic Writes](experiments/monotonic_writes/README.md)
- [Read Your Writes](experiments/read_your_writes/README.md)
- [Writes Follow Reads](experiments/writes_follow_reads/README.md)

Each experiment README describes the experimental setup, consistency-level configurations, failure scenarios, commands required to run the experiment, and output interpretation.

## Results

Experimental results are stored under:

- `results/monotonic_reads/`
- `results/monotonic_writes/`
- `results/read_your_writes/`
- `results/writes_follow_reads/`

Refer to the corresponding experiment README for details on the result files and how they were generated.