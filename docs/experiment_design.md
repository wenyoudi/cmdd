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
Writes issued by the same client should be applied in the order in
which they were issued.

Violation condition:
Client A successfully performs W1(x, 1) followed by W2(x, 2), but
after both writes have completed, an observation exposes the W1-only
state (version 1) rather than the later version 2. In this experiment,
such an observation is treated as a possible Monotonic-Writes ordering
anomaly.

A pre-write value (version 0) is treated as stale replica state rather
than, by itself, evidence of a Monotonic-Writes ordering violation.

Candidate experiment:
For each trial, initialize a unique key to version 0 using consistency
level ALL. Client A then performs two successive writes:

W1(x, 1) -> W2(x, 2)

W2 is issued only after W1 succeeds. W1 is coordinated through node 1
and W2 through node 2.

The following write consistency-level combinations are tested:

- ONE / ONE
- ONE / QUORUM
- QUORUM / ONE
- QUORUM / QUORUM
- ALL / ONE
- ONE / ALL

The experiment is repeated under three scenarios:

1. Normal operation:
   All three Cassandra nodes are available.

2. Node failure:
   Node 3 is stopped. Successful writes are verified through the two
   surviving nodes.

3. Network partition:
   Cassandra internode communication between node 3 and nodes 1 and 2
   is blocked while CQL access to node 3 remains available. This allows
   the experiment to observe replica divergence during the partition.

After W1 and W2 both succeed, verification reads use consistency level
ONE through the relevant nodes.

Observed versions are interpreted as follows:

- version 2: latest state after W1 followed by W2
- version 1: possible Monotonic-Writes ordering anomaly under the
  experimental criterion
- version 0: stale/pre-write replica state, not by itself an MW
  ordering violation

A failed write caused by an unavailable consistency level is recorded
as an availability failure rather than a consistency violation.

The experiment is repeated for 10 trials per consistency-level
configuration.

Note:
The experiment uses explicitly increasing Cassandra timestamps.
Because Cassandra uses timestamp-based last-write-wins conflict
resolution, this may help preserve the intended W1 -> W2 order.
Therefore, failure to observe an anomaly does not establish a general
Monotonic-Writes guarantee.

### Writes Follow Reads

Definition:
TODO

Violation condition:
TODO

Candidate experiment:
TODO
