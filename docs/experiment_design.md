# Experiment Design

## Cluster

- Database: Apache Cassandra 5.0.x
- Nodes: 3
- Replication factor: 3
- Datacenter: datacenter1

## Consistency Levels

The cluster uses a replication factor of N = 3.

### ONE
For a write, ONE requires at least one replica to acknowledge the write before the coordinator reports success.

For a read, ONE quires a response from one replica.

This provides relatively high availability but may allow a client to observe stale data because the replica serving a read may not be contain the latest successful write. 

### QUORUM

For a replication factor N = 3, a quorum consists of:


floor(N / 2) + 1 = 2 replicas.

Therefore, QUORUM requires responses from at least two replicas.

If both reads and writes use QUORUM:

R + W = 2 + 2 = 4 > 3

so the read and write replica sets must overlap.

### ALL
ALL requires responses from all three replicas.

For writes, all three replicas must acknowledge the write before it is
reported successful.

For reads, all three replicas must respond.

This provides stronger consistency behaviour but lower availability:
if even one replica is unavailable, an ALL operation cannot complete.

## Client-Centric Consistency Models

### Read Your Writes

Definition:
After a client successfully performs a write, subsequent reads by that
same client should observe that write or a newer value.

Violation condition:
Client A successfully writes version v, but a subsequent read by
Client A returns version u where u < v.

Candidate experiment:
TODO

### Monotonic Reads

Definition:
Once a client has observed a particular version of a value, subsequent
reads by that client should not return an older version.

Violation condition:
Client A reads version v and subsequently reads version u where u < v.

Candidate experiment:
TODO

### Monotonic Writes

Definition:
TODO

Violation condition:
TODO

Candidate experiment:
TODO

### Writes Follow Reads

Definition:
TODO

Violation condition:
TODO

Candidate experiment:
TODO