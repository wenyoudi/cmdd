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
A write issued by a client after reading a value should be ordered after, and causally depend on, the version observed by that read.

Violation condition:
Client B successfully writes an antecedent value parent = 1. Client A subsequently reads parent = 1 and then successfully performs a dependent write child = 1. After both operations have completed, an observation exposes child = 1 while still exposing parent = 0.

In this experiment, the state (parent = 0, child = 1) is treated as a possible Writes-Follow-Reads causal-ordering anomaly.

A state such as (parent = 0, child = 0) is treated as stale replica state rather than, by itself, evidence of a WFR violation. Failed reads or writes caused by unavailable consistency levels are recorded as availability failures rather than consistency violations.

Candidate experiment:
For each trial, initialize a unique key to parent = 0 and child = 0 using consistency level ALL. Client B first writes parent = 1 using consistency level QUORUM. Client A then reads parent, and the causal dependency is considered established only if Client A successfully observes parent = 1. Client A then performs the dependent write child = 1:

B: W(parent, 1) -> A: R(parent, 1) -> A: W(child, 1)

The antecedent write is fixed at QUORUM so that the experiment varies only the consistency levels of Client A's dependency read and dependent write.

The following read / dependent-write consistency-level combinations are tested:

- ONE / ONE
- ONE / QUORUM
- QUORUM / ONE
- QUORUM / QUORUM
- ALL / ONE
- ONE / ALL

The experiment is repeated under three scenarios:

1. Normal operation: All three Cassandra nodes are available. The dependent write is coordinated through node 2, and the final state is verified through all three nodes.
2. Node failure: Node 3 is stopped. The dependency read is performed through node 1, the dependent write is coordinated through node 2, and successful trials are verified through the two surviving nodes.
3. Network partition: Cassandra internode communication between node 3 and nodes 1 and 2 is blocked while CQL access to node 3 remains available. The antecedent write and dependency read are performed through node 1 on the node 1/node 2 side, while the dependent write is coordinated through the isolated node 3. This allows the experiment to test whether a dependent write can become visible on node 3 without the antecedent value being visible there.

After the dependency has been established and the dependent write succeeds, verification reads use consistency level ONE through the relevant nodes. Each verification jointly reads parent and child so that both values belong to the same observation.

Observed states are interpreted as follows:

- parent = 1, child = 1: both the antecedent and dependent write are visible
- parent = 1, child = 0: the antecedent is visible but the dependent write is not visible at that observation point
- parent = 0, child = 0: stale/pre-write replica state
- parent = 0, child = 1: possible Writes-Follow-Reads anomaly under the experimental criterion

If the dependency read fails or does not observe parent = 1, the dependency is not established and the dependent write is not used to claim a WFR violation. If the dependent write fails, the trial is recorded as an availability failure rather than a consistency violation.

The experiment is repeated for 10 trials per consistency-level configuration.

Prediction: Under normal operation, no WFR anomaly is expected. Under node failure, operations using ONE or QUORUM are expected to remain available on the two surviving replicas, while operations requiring ALL may fail. Under network partition, when the dependent write uses ONE through the isolated node 3, node 3 may accept child = 1 while still exposing parent = 0, producing the target WFR anomaly pattern. Stronger dependent-write consistency levels such as QUORUM or ALL are expected to become unavailable on the isolated node rather than allow that write to complete.

Note: The observation (parent = 0, child = 1) is used as an experimental signal of a possible causal-visibility anomaly. Observing this pattern in the experiment should not be interpreted as a general proof that Cassandra violates WFR in every configuration. Similarly, failure to observe the pattern does not establish a general WFR guarantee.
