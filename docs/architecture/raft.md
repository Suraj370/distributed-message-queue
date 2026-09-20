# Raft implementation

A from-scratch Raft implementation (`raft/` package) covering leader election and log replication,
per the [Raft paper](https://raft.github.io/raft.pdf). This document covers the mechanism itself;
see [replication.md](replication.md) for how a committed entry becomes actual queue state, and
[../design/storage.md](../design/storage.md) for how the log/metadata are made durable.

## Node state

`RaftNode` holds the state every Raft node needs: `currentTerm`, `state` (`FOLLOWER` / `CANDIDATE`
/ `LEADER`), `votedFor`, a `RaftLog`, `commitIndex`, `lastApplied`. `currentTerm`/`votedFor` are
recovered from a `RaftMetadataStore` on construction; a recovered node **always** starts
`FOLLOWER` regardless of what it was before a restart - it must rejoin a real election to become
leader again, never assume it still is one.

State transitions are deliberately narrow methods, each enforcing its own precondition:
`becomeCandidate()` only from `FOLLOWER`, `becomeLeader()` only from `CANDIDATE`,
`becomeFollower()` only from `CANDIDATE` (a follower is never voluntarily "stepped down to" - it
only happens via `advanceTerm()`, i.e. seeing a higher term). `advanceTerm(newTerm)` is the one
place `votedFor` resets to `null` and state resets to `FOLLOWER` - any RPC (`RequestVote`,
`AppendEntries`, `Heartbeat`) carrying a higher term drives this.

`commitIndex` only ever advances (`advanceCommitIndex`, clamped to the local log's length) and
`lastApplied` only ever advances up to `commitIndex` (`markApplied`, throws if asked to skip ahead
of what's committed).

## Election

Three small, separately-testable pieces:

- **`ElectionTimeout`** - a randomized deadline in `[minMillis, maxMillis)`, re-rolled on every
  `reset()`. Time is injected (`nowMillis` parameters throughout), not read from a wall clock, so
  election-timeout behavior is deterministic to unit test.
- **`ElectionCoordinator`** - one synchronous election attempt: become `CANDIDATE`, request votes
  from every configured peer in sequence, become `LEADER` on a majority (computed from the
  cluster's *configured* size, never from how many peers happened to answer - an unreachable
  member must not silently shrink the majority requirement). Bails out early if a higher term
  arrives or another election for this node starts concurrently.
- **`RaftElectionDriver`** - ties an `ElectionTimeout` to an `ElectionCoordinator` for one node.
  `tick(nowMillis)` is a no-op for a `LEADER`; a `FOLLOWER`/`CANDIDATE` whose timeout elapsed
  starts a fresh election (a `CANDIDATE` first steps back to `FOLLOWER` so `becomeCandidate()`'s
  precondition holds). `onHeartbeatReceived`/`onAppendEntriesReceived` reset the timeout only when
  the RPC was actually accepted from a current/newer-term leader - a stale or rejected RPC must
  never extend a follower's patience for its actual leader.

**No PreVote.** A node whose own timeout fires before it hears from a healthy leader will still
increment its term and force a real election, even if it can never win one (a behind candidate is
always rejected by `isCandidateLogUpToDate`, Raft's log-comparison rule in `handleRequestVote`).
The cluster always recovers correctly, but this can cause a brief, legitimate leadership churn -
observed repeatedly during this project's own Docker demo testing (see `docker-compose.yml`'s
README section) whenever a broker restarts. Adding a PreVote phase would remove it; that's a change
to the election algorithm, deliberately out of scope so far.

## Log

`RaftLog` is a 1-indexed, in-memory log (index 0 is the Raft paper's "before the log" sentinel for
`prevLogIndex`). `appendEntries(prevLogIndex, prevLogTerm, newEntries)` applies the whole Raft
log-matching/conflict-resolution rule as one atomic, `synchronized` operation: reject unless
`prevLogIndex`/`prevLogTerm` match; then for each new entry, leave an already-present entry with a
matching term alone (idempotent under a retried/duplicate `AppendEntries`), or truncate the
conflicting suffix and append. `DurableRaftLog` wraps this with a durable, crash-safe mirror to a
`RaftLogStore` - see [../design/storage.md](../design/storage.md).

## Replication

`RaftLogReplicator` is leader-only. For each peer it tracks `nextIndex`/`matchIndex`, sends
`AppendEntries` with everything from `nextIndex` onward, and on rejection backs `nextIndex` off by
one and retries (bounded: `prevLogIndex` 0 always matches, so this always terminates). After a
round, it advances the leader's own `commitIndex` using the majority rule - restricted to entries
from the leader's *current* term, per the Raft paper's commit-safety rule (an older-term entry only
becomes committed indirectly, by a later current-term entry committing "over" it - this is why a
freshly-elected leader's `commitIndex` can briefly read behind the cluster's even though its log
already has everything, until it commits something new itself).

`initializeForNewLeader()` must run exactly once per new leadership term, before the first
`replicate()` call - `RaftElectionDriver`'s `onBecomeLeader` hook wires this automatically.

See [replication.md](replication.md) for how replication is kept flowing continuously (not just on
a publish) and how a follower actually applies what it receives.

## Real-time wiring

`RaftNodeScheduler` is the only class that owns a thread - one daemon `ScheduledExecutorService`
ticking at `heartbeatIntervalMillis`, each tick checking the election timeout, broadcasting a
heartbeat if leader, then running an injected `onTick` hook (real wiring: catch-up replication
while leader). Every other Raft class is deterministic and time-injected specifically so it can be
unit tested without this scheduler running at all.

## Transport

`raft.transport` carries the same `RequestVoteRequest`/`AppendEntriesRequest`/`Heartbeat` types
over real HTTP between brokers (`HttpRaftPeerConnection`, `HttpAppendEntriesConnection`,
`HttpHeartbeatConnection`, all under `/internal/raft/*`, distinct from the client-facing
`/api/v1/*` surface). Every connection type is built to never throw back to its caller - an
unreachable peer, a malformed response, or a remote error all become a safe non-granting/
non-successful response object instead, so one bad peer can never crash the caller's Raft loop.

## Known limitations

- No PreVote (above).
- **A leader isolated from the majority does not step down on its own.** `propose()` correctly
  throws `QuorumUnavailableException` and applies nothing, but the isolated node keeps calling
  itself `LEADER` locally until restarted or the partition heals.
- Cluster membership is static and configured, not dynamically reconfigurable.
