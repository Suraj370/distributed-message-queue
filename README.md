# Fault-Tolerant Distributed Message Queue

A fault-tolerant distributed message queue built using Java, Spring Boot, and Gradle.

## Goals

- Concurrent message producers and consumers
- Partitioned topics
- Persistent message storage
- Consumer groups
- Message replication
- Leader election
- Fault tolerance
- Crash recovery
- Performance benchmarking

## Tech Stack

- Java
- Spring Boot
- Gradle
- JUnit
- Docker
- Testcontainers

## Status

🚧 Under development

## Architecture

```
Client
  |
  v
Spring Boot API (TopicController, MessageController)
  |
  v
Broker
  |
  v
RaftReplicatedQueue.propose(PublishCommand)
  |
  v
RaftNode (leader check) -- throws NotLeaderException if not leader
  |
  v
RaftLog.appendCommand()               <-- Raft's replicated consensus log
  |
  v
RaftLogReplicator.replicate()  --AppendEntries-->  follower RaftNodes
  |
  v
majority commit (RaftNode.advanceCommitIndex)
  |
  v
RaftLogApplier.applyCommitted()  -- in strict index order, exactly once per node
  |
  v
RaftQueueStateMachine.apply()          <-- decodes PublishCommand, the ONLY caller of Partition.append()
  |
  v
Partition                               <-- queue mutation (in-memory + offset index)
  |
  v
Wal                                     <-- durable queue storage
```

**The key invariant:** a message never becomes committed queue state merely because a leader
received it. `RaftReplicatedQueue.propose()` only calls `RaftLogApplier.applyCommitted()` (which
is the only path into `Partition.append()`) after `RaftNode.getCommitIndex()` has actually reached
that entry's index - i.e. after a majority of the *configured* cluster (not just currently
reachable peers) has durably stored it.

### Components

1. **Broker** - the application-facing service (`com.surajpanda.dmq.broker`). Creates topics
   directly (topic creation is not yet Raft-replicated - see Limitations) and routes every publish
   through `RaftReplicatedQueue`.
2. **Topics** (`com.surajpanda.dmq.topic`) - a named collection of partitions, with metadata
   (name + partition count) durably recorded in `topics.meta` and recovered on startup.
3. **Partitions** (`com.surajpanda.dmq.partition`) - each partition owns an in-memory message
   queue (`poll()`, at-most-once), an offset-indexed map (`get()`/`readFrom()`, non-destructive,
   backing at-least-once), and its own WAL.
4. **WAL** (`com.surajpanda.dmq.wal`) - durable, append-only queue storage. One record per
   message. A corrupted/incomplete final record is discarded on recovery; a corrupted record
   earlier in the file is a hard failure.
5. **Consumer groups & offsets** (`com.surajpanda.dmq.consumer`) - a group's committed offset is
   tracked per (topic, partition), durably in `FileOffsetStore` (one append-only file per group,
   last-write-wins on replay), independent of any particular `Partition` object's lifetime.
6. **Raft** (`com.surajpanda.dmq.raft`) - `RaftNode` owns term/votedFor/log/commitIndex/lastApplied.
   Elections (`ElectionCoordinator`), heartbeats (`HeartbeatBroadcaster`), and log replication
   (`RaftLogReplicator`, with per-peer nextIndex/matchIndex and backtracking on log mismatch) are
   separate orchestrator classes over the same `RaftNode`. `currentTerm`/`votedFor` persist via
   `FileRaftMetadataStore`; the log persists via `DurableRaftLog` + `FileRaftLogStore` (both
   append-only, in the same style as the queue's WAL).
7. **Replication** - majority calculation always uses the *configured* cluster size
   (`clusterSize`, passed explicitly to `ElectionCoordinator`/`RaftLogReplicator`), never the
   number of currently-reachable peers - an unreachable member must never silently shrink the
   majority requirement.
8. **State-machine application** (`com.surajpanda.dmq.queue.RaftQueueStateMachine`) - applies
   committed `PublishCommand` entries to `Partition` in strict index order, exactly once per
   process lifetime for each Raft log index (tracked via its own durable `lastAppliedIndex`,
   independent of `RaftNode`'s in-memory `lastApplied`, precisely so a restart can never
   re-apply - and duplicate - an already-applied command).
9. **Failure recovery** - a Raft node recovers `currentTerm`/`votedFor`/log from disk on restart
   and always starts `FOLLOWER`; a restarted follower catches up via the normal AppendEntries
   backtracking path, the same mechanism used for a follower that was simply behind.

### Delivery semantics

This project deliberately does **not** claim exactly-once delivery. Two semantics are explicitly
implemented and tested (`DeliverySemanticsTest`):

- **At-most-once** - `Consumer.poll()`. Destructively removes the next message. If the caller
  crashes after `poll()` returns but before finishing processing, that message is lost - it was
  already removed.
- **At-least-once** - `Consumer.fetchNext(group)` + `ConsumerGroup.commitOffset(...)`.
  `fetchNext()` never removes or advances anything; only an explicit `commitOffset()` call (made
  *after* processing succeeds) advances the group's durable offset. A crash between fetch and
  commit means the same message is fetched again on the next attempt.

Consumers that need effectively-once *processing* on top of at-least-once delivery must make their
own processing idempotent (e.g. dedupe by message id) - this project does not provide that for you.

### Cluster membership and consensus scope

- Cluster membership is **static** for this milestone - no dynamic reconfiguration (adding/removing
  voting members at runtime) is implemented.
- No Raft snapshotting or log compaction is implemented yet - the log grows unbounded.
- No external message broker (Kafka, RabbitMQ, Redis, etc.) is used anywhere - the queue engine and
  Raft implementation are entirely this project's own Java code.

### Known limitations / next-milestone concerns

- **No real network transport between brokers.** There is no HTTP/RPC client for
  RequestVote/AppendEntries - every Raft test in this codebase (including the 3/5-node majority and
  distributed-failure tests) runs multiple `RaftNode`s in-process, wired together with in-process or
  fault-injecting connections. The Spring-wired `RaftConfiguration` therefore runs each broker as
  its own single-node Raft "cluster" (`clusterSize=1`) regardless of how many peers are listed under
  `cluster.brokers` - every publish still genuinely goes through propose -> replicate ->
  majority-commit -> apply, just against a trivial one-node majority. Real multi-broker replication
  requires building that transport layer first.
- **Topic creation is not yet Raft-replicated** - only `PublishCommand`s are. Every node in a real
  multi-broker deployment would need identical topic configuration out of band until topic creation
  is proposed through Raft as well.
- Observability is limited to plain getters (`RaftNode.getCurrentTerm()/getState()/getCommitIndex()
  /getLastApplied()`, `RaftLogReplicator.getMatchIndex()`, `Partition.size()`,
  `ConsumerGroup.lag()`) - no metrics endpoint or dashboard yet.