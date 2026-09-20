# Architecture overview

A fault-tolerant distributed message queue: topics split into partitions, each partition a durable,
offset-indexed log; a Raft consensus group per broker cluster replicates every write to a majority
before it's visible; consumer groups track committed offsets independently of the log itself.

This document is a map. For depth on a specific area see:

- [partitioning.md](partitioning.md) - topics, partitions, key-to-partition routing
- [raft.md](raft.md) - the Raft implementation itself (election, log, replication)
- [replication.md](replication.md) - how a commit actually becomes queue state on every broker
- [../design/storage.md](../design/storage.md) - on-disk formats and durability
- [../design/concurrency.md](../design/concurrency.md) - locking and threading model
- [../design/delivery-semantics.md](../design/delivery-semantics.md) - at-most-once vs at-least-once
- [../testing/failure-testing.md](../testing/failure-testing.md) - how correctness under failure is verified
- [../testing/benchmarks.md](../testing/benchmarks.md) - JMH performance baselines

## Components

| Package | Responsibility |
|---|---|
| `message` | `Message` - the record stored in a partition's WAL |
| `wal` | `Wal` - synchronous, `fsync`'d append-only message log for one partition |
| `partition` | `Partition` - one topic-partition's in-memory index plus its WAL |
| `topic` | `Topic`, `TopicManager` - topic creation/lookup, owns each topic's partitions |
| `broker` | `Broker` (publish/read facade), `PartitionSelector` (key hashing), `*Configuration` (Spring wiring), `*Properties` |
| `cluster` | `ClusterProperties`, `BrokerAddress` - configured cluster membership |
| `raft` | The Raft implementation itself - node state, election, log, replication, HTTP transport |
| `queue` | `RaftReplicatedQueue`, `RaftQueueStateMachine` - bridges Raft's generic log to this project's `PublishCommand`s and `Partition`s |
| `consumer` | `Consumer`, `ConsumerGroup`, offset stores - at-least-once consumption |
| `replication` | `PartitionReplicator` and friends - an earlier, non-Raft replication mechanism; **not used by the running application** (see [replication.md](replication.md)) |
| `api` | REST controllers - the only way a client or another broker talks to this process |
| `benchmarks` (`src/jmh`) | JMH microbenchmarks - see [benchmarks.md](../testing/benchmarks.md) |

## Request flow: a publish, end to end

1. `MessageController` (HTTP) calls `Broker.publish(topic, key, payload)`.
2. `Broker` resolves the topic, picks a partition via `PartitionSelector.select(key.hashCode(), partitionCount)`,
   and hands a `PublishCommand` to `RaftReplicatedQueue.propose(...)`.
3. `RaftReplicatedQueue` requires this node to be the Raft `LEADER` (else `NotLeaderException`),
   appends the command to the local Raft log (`RaftNode.getLog().appendCommand(...)`), and drives
   `RaftLogReplicator.replicate(...)` until a majority of the configured cluster has stored the
   entry (else `QuorumUnavailableException`).
4. Once committed, `RaftLogApplier.applyCommitted()` walks every newly-committed log entry through
   `RaftQueueStateMachine.apply(entry)`, which decodes the `PublishCommand` and calls
   `Partition.append(key, payload, raftLogIndex)` - the point where the message actually lands in
   the WAL and becomes visible to consumers.
5. The same `apply()` call happens on every follower too, driven by `RaftInternalController` right
   after each `AppendEntries` it handles - see [replication.md](replication.md) for why that, not
   just committing the log, is what makes followers' queue state converge with the leader's.

A fetch/commit follows a much shorter path: `ConsumerController` → `Consumer.fetchNext(group)` (a
non-destructive, offset-indexed `Partition.get(offset)` read) → `ConsumerGroup.commitOffset(...)`
on success. Neither goes through Raft - offsets are per-broker-group state, not replicated queue
data (see [delivery-semantics.md](../design/delivery-semantics.md)).

## What is and isn't Raft-replicated

Only `PublishCommand`s go through Raft. Topic creation (`TopicManager.createTopic`) is local,
in-process, append-only metadata - every broker in a real cluster currently needs identical topic
configuration applied to it directly (see the README's "Known limitations" and its Docker demo
flow, which creates each topic on all three brokers before publishing).

## Observability and benchmarking

Runtime metrics (Micrometer/Prometheus/Grafana) and JMH benchmarks are deliberately separate
concerns from this architecture - see the README's Observability section and
[benchmarks.md](../testing/benchmarks.md) for what each answers.
