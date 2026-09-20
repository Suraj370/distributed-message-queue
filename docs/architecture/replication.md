# Replication

How a publish accepted by the leader becomes durable queue state on every broker. This is the
project's real, current replication mechanism - entirely built on Raft (see [raft.md](raft.md)).

## The path from commit to applied state

Raft on its own only guarantees a majority of brokers durably *store* a log entry, and that the
leader's `commitIndex` correctly reflects that. It says nothing about a state machine having
*applied* that entry yet. This project closes that gap with two cooperating pieces:

- **`RaftLogApplier.applyCommitted()`** walks every entry between a node's `lastApplied` and its
  `commitIndex`, in strict order, calling `RaftStateMachine.apply(entry)` for each and advancing
  `lastApplied` as it goes. Safe to call repeatedly (e.g. after every `commitIndex` advancement) -
  it only ever walks forward.
- **`RaftQueueStateMachine`** (the concrete `RaftStateMachine`) decodes the entry's `PublishCommand`
  and calls `Partition.append(key, payload, raftLogIndex)` - the *only* place a Raft-proposed
  publish ever reaches a `Partition`/WAL. It runs identically on the leader and every follower, so
  applying the same committed log in the same order is what makes their queue state converge.

## Who drives `applyCommitted()`, and when

- **On the leader**, `RaftReplicatedQueue.propose()` calls it directly, synchronously, right after
  confirming the entry committed - so a publish response only returns once the leader itself has
  durably applied the message.
- **On a follower**, nothing about a publish ever reaches it directly. `RaftInternalController`
  calls `applyCommitted()` right after handling *every* incoming `AppendEntries` request - whether
  that request carried new entries or was just a heartbeat-shaped catch-up round with nothing new,
  because the follower's `commitIndex` may have advanced from the leader's `leaderCommit` field even
  when no new log entries were needed for this go-around.

## Continuous replication, not just on publish

`RaftLogReplicator.replicate()` is driven from two places:

1. **A client publish**, via `RaftReplicatedQueue.propose()` - the request-thread path, replicating
   the new entry immediately.
2. **Every scheduler tick while leader** (`RaftNodeScheduler`'s `onTick` hook, wired in
   `RaftConfiguration`) - a periodic catch-up round independent of any new publish. This is what
   lets a follower that fell behind (e.g. it just restarted, or missed entries while partitioned)
   converge on its own, without needing a new client publish to trigger it. A tick with nothing new
   to send is cheap: `replicate()` just re-confirms `matchIndex`/`leaderCommit` for an already
   caught-up peer.

Both call sites are serialized through the same `synchronized` methods on `RaftLogReplicator`, so
the per-peer `nextIndex`/`matchIndex` bookkeeping stays consistent regardless of which thread
triggered a given round.

## A note on `com.surajpanda.dmq.replication`

The `replication` package (`PartitionReplicator`, `ReplicaConnection`, `ReplicationOutcome`, ...)
predates the Raft-based design above and **is not wired into the running application** - grep
`RaftConfiguration`/`BrokerConfiguration` and it never appears. Its own unit tests
(`PartitionReplicatorTest`, `ReplicationFailureTest`, `ReplicationPartitionIndependenceTest`) still
pass and still exercise that class directly, but they are testing a superseded mechanism, not the
production replication path documented above and in [raft.md](raft.md). Treat this document and
`raft.md` as authoritative for how replication actually works today.
