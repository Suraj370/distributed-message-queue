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

## Run with Docker

The fastest way to see the whole system - a real 3-broker Raft cluster, Prometheus, and Grafana -
running together on one machine.

**Prerequisites**: Docker, Docker Compose (`docker compose version`).

**Start:**

```
docker compose up --build
```

**Ports:**

| Service | Port | Purpose |
|---|---|---|
| broker-1 | `8080` | HTTP API (`/api/v1/...`), Actuator (`/actuator/health`, `/actuator/prometheus`) |
| broker-2 | `8081` | Same, this broker's own port |
| broker-3 | `8082` | Same, this broker's own port |
| Prometheus | `9090` | Prometheus UI/API |
| Grafana | `3000` | Dashboard UI (anonymous admin access - local demo only) |

Internally, brokers always reach each other over the Docker Compose network by service name
(`broker-1`/`broker-2`/`broker-3`, always port `8080` - see `docker-compose.yml`), never
`localhost` and never the host-mapped ports above, which only exist for a human on the host
machine to reach things.

**Stop:** `docker compose down` (add `-v` to also delete the named volumes and start completely
fresh next time - see Persistence below).

**Persistence:** each broker gets its own named Docker volume (`broker-1-data`, `broker-2-data`,
`broker-3-data`) mounted at `/data`, holding its WAL, Raft log/metadata, and consumer offsets. A
`docker compose restart <broker>` (or `stop` + `start`) reuses the same volume, so a broker's
durable state - and the cluster's committed data - survives a restart exactly as the existing
recovery logic expects (see "Automatic follower catch-up" above). A `docker compose down` without
`-v` also preserves the volumes across a full stack teardown/recreate.

**Broker logs:** `docker compose logs -f broker-1` (or `broker-2`/`broker-3`; drop `-f` for a
one-shot dump). `docker compose ps` shows each broker's health-check status (`healthy`/
`unhealthy`/`starting`).

### Demonstration flow

All commands use the real HTTP API already documented above - nothing here is invented.

1. **Start the cluster:**
   ```
   docker compose up --build
   ```
2. **Verify the brokers become healthy:**
   ```
   docker compose ps
   ```
   Each broker should show `(healthy)` within about 30 seconds (Docker's `HEALTHCHECK`, defined in
   the Dockerfile, polls `/actuator/health`).
3. **Create a topic on every broker** (topic creation is not yet Raft-replicated - see Known
   limitations - so every broker needs it created directly for now):
   ```
   for p in 8080 8081 8082; do curl -X POST "http://localhost:$p/api/v1/topics?name=demo&partitions=1"; done
   ```
4. **Publish messages** (only the current Raft leader accepts a publish; check
   `/api/v1/status` on each port first, or just retry against another port if one returns
   `NOT_LEADER`):
   ```
   curl -X POST "http://localhost:8080/api/v1/messages?topic=demo&key=k1&payload=hello"
   ```
5. **Consume and commit:**
   ```
   curl -X POST "http://localhost:8080/api/v1/consumer-groups/g1/fetch?topic=demo&partition=0"
   curl -X POST "http://localhost:8080/api/v1/consumer-groups/g1/commit?topic=demo&partition=0&offset=1"
   ```
6. **Open Grafana** at <http://localhost:3000> - the "DMQ Cluster" dashboard is already provisioned
   (DMQ folder); the top-row publish rate and append latency panels move immediately after step 4.
7. **Stop a broker** (whichever `/api/v1/status` reports as `"raftState":"LEADER"`):
   ```
   docker compose stop broker-1
   ```
8. **Observe the Raft leader change** - either poll `/api/v1/status` on the two remaining brokers,
   or watch the dashboard's "Broker Raft state" and "Current leader" panels flip to the newly
   elected broker within a few seconds.
9. **Restart the stopped broker:**
   ```
   docker compose start broker-1
   ```
10. **Observe recovery/catch-up** - `/api/v1/status` on the restarted broker shows it rejoin as
    `FOLLOWER` and its `commitIndex`/`lastApplied` converge back to the cluster's, with no data
    loss (its WAL/Raft-log files under `/data` were never touched by the restart).

## Architecture

```
        Client
           │
           ▼
   Spring API (TopicController, MessageController, ConsumerController, StatusController)
           │
           ▼
        Broker 1  (Raft leader)
           │
      Raft HTTP (/internal/raft/*)
      ┌────┴────┐
      ▼         ▼
 ┌─────────┐ ┌─────────┐
 │Broker 2 │ │Broker 3 │  (Raft followers)
 └─────────┘ └─────────┘
           │
           ▼  (on the leader, once a majority has stored the entry)
   Queue state machine (RaftQueueStateMachine.apply())
           │
           ▼
       Partition  →  Queue WAL
```

Every broker in a cluster runs the same pipeline. A follower receives AppendEntries over the real
HTTP transport (see "Broker-to-broker transport" below), does its own election-timeout bookkeeping,
and - once a request from the current leader advances its commitIndex - drives its own
`RaftLogApplier`/`RaftQueueStateMachine` locally. Leader and every follower converge to the same
queue state by replaying the same committed log, never by copying queue state directly.

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
10. **Transport** (`com.surajpanda.dmq.raft.transport`) - the real broker-to-broker network layer.
    `HttpRaftPeerConnection`/`HttpAppendEntriesConnection`/`HttpHeartbeatConnection` implement the
    same `RaftPeerConnection`/`AppendEntriesConnection`/`HeartbeatConnection` interfaces the Raft
    classes already depended on by POSTing to another broker's internal endpoints, exposed by
    `RaftInternalController` under `/internal/raft/{request-vote,append-entries,heartbeat}` - never
    under the client-facing `/api/v1` path.
11. **Status/read API** (`com.surajpanda.dmq.api`) - `StatusController` exposes each broker's own
    Raft state/term/commitIndex read-only over `GET /api/v1/status`; `MessageController` gained a
    non-destructive `GET /api/v1/messages` (read at a specific offset); `ConsumerController` is a
    thin HTTP facade over `Consumer`/`ConsumerGroup` (`POST .../fetch`, `POST .../commit`). All
    added so an external caller - an operator, or a Testcontainers-style test - can observe cluster
    state and exercise publish/consume without ever reaching into broker internals.

### Persistence responsibilities

| Concern | Durable store | Notes |
| --- | --- | --- |
| Raft `currentTerm`/`votedFor` | `FileRaftMetadataStore` | write-temp-then-atomic-rename |
| Raft replicated log | `FileRaftLogStore` (via `DurableRaftLog`) | every mutation persisted as one complete-log temp-file-then-atomic-rename `replace()`, not an incremental append - see below |
| Raft state-machine progress | `FileLastAppliedStore` | a fast-path skip only - see below, not the actual duplicate-prevention mechanism |
| Queue messages | `Wal` (per partition) | each message optionally tagged with the Raft log index that produced it |
| Consumer offsets | `FileOffsetStore` | append-only, last-write-wins per (group, topic, partition) |

**Durable Raft log crash consistency.** `RaftLogStore.replace(fullLog)` persists the *complete*
intended logical log as a single durable operation - not an incremental append/truncate record.
`FileRaftLogStore` implements this as: serialize the whole log, fully write and fsync it to a
sibling temp file, then atomically rename that temp file over the real file
(`ATOMIC_MOVE`+`REPLACE_EXISTING`, with a documented-weaker non-atomic fallback only if the
filesystem genuinely lacks atomic same-directory moves). `DurableRaftLog` applies a mutation to the
in-memory log first (the inherited, unmodified `RaftLog` algorithm), then calls `replace()`; if that
throws, `RaftLog.restoreSuffix()` rolls memory back to exactly what is durable. This closes a real
gap an earlier append-only design had: `FileChannel.write()` can make partial progress, so an
incremental multi-entry mutation could end up with entry A durably persisted while entry B was not,
even though the caller only saw one exception. Routing every mutation through a temp file that is
either fully promoted or not promoted at all removes that failure mode: old durable state *or*
complete new durable state, never a partial mutation. This does not claim immunity from a true
OS/power crash mid-syscall truncating the temp file - that narrower case is simply never promoted,
so the real file is untouched either way; recovery still treats any corruption actually found in the
real file as a hard failure, since a partially-written real file is no longer an expected artifact
of normal operation.

**Queue state-machine idempotency.** A naive "append to WAL, then save lastAppliedIndex" sequence is
not crash-safe: a crash between the two leaves `lastAppliedIndex` stale, and re-applying the same
committed entry after restart would duplicate the message. Instead, each queue `Message` produced by
applying a committed command is tagged with that command's Raft log index (`Message.raftLogIndex`),
durably persisted through the WAL. `RaftQueueStateMachine.apply()` checks
`Partition.hasAppliedRaftIndex()` - backed by the WAL itself, not by `lastAppliedIndex` - before
appending, so `lastAppliedIndex` remains only a fast-path skip and the WAL is the actual source of
truth for "was this committed command already applied."

### Broker-to-broker transport

`RaftInternalController` exposes three endpoints under `/internal/raft`, one per Raft RPC this
codebase already models as a distinct interface: `POST /internal/raft/request-vote`, `POST
/internal/raft/append-entries`, `POST /internal/raft/heartbeat`. They are deliberately kept off the
client-facing `/api/v1` path and are never meant to be called by an external client.

- `request-vote` goes straight to `RaftNode.handleRequestVote` (granting a vote does not reset the
  responder's own election timeout in this implementation, matching every in-process connection).
- `append-entries` and `heartbeat` are routed through `RaftElectionDriver`, so a request from a
  current/newer-term leader resets the receiving follower's election timeout; `append-entries`
  additionally drives that follower's own `RaftLogApplier.applyCommitted()` afterward.
- A blank `candidateId`/`leaderId` is rejected with `400`; every other failure is handled by the
  same `GlobalExceptionHandler` the client-facing API uses - no stack trace is ever returned.

`HttpRaftPeerConnection`/`HttpAppendEntriesConnection`/`HttpHeartbeatConnection` convert every
network-level failure (refused/timed-out connection, 5xx, a malformed or empty response body) into a
safe non-granting/non-success response at the *request's own term* - never a thrown exception, and
never a manufactured higher term - so an unreachable peer can neither crash the caller nor be
mistaken for a legitimate response. Timing is configurable via `raft.transport.connect-timeout-
millis`/`read-timeout-millis` and `raft.election.min-timeout-millis`/`max-timeout-millis`/
`raft.heartbeat-interval-millis`. Protocol-level retries (log backtracking on mismatch,
majority-commit bookkeeping) remain entirely the responsibility of the existing Raft classes.

### Automatic follower catch-up

`RaftNodeScheduler` takes an optional `onTick` hook, run every scheduler tick after
election/heartbeat bookkeeping; `RaftConfiguration` wires it to call `RaftLogReplicator.replicate()`
whenever this broker is currently `LEADER`. This is what lets a follower that fell behind (e.g. it
just restarted) converge automatically on the next tick, rather than only when the next client
publish happens to trigger a replication round. `RaftLogReplicator`'s per-peer nextIndex/matchIndex
bookkeeping is `synchronized`, since a client-request thread and the scheduler thread can now call
`replicate()` concurrently. `RaftReplicatedQueue.propose()` translates the `IllegalStateException`
`replicate()` throws if this node stepped down between propose()'s own leader check and the
replicate() call into `NotLeaderException`, so that race surfaces as the documented exception
contract instead of an unhandled `IllegalStateException`.

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

- Cluster membership is **static and configured**, via `cluster.brokers` (`ClusterProperties`/
  `BrokerAddress`) - every broker in the cluster lists the same fixed set of `{id, host, port}`
  entries. `RaftConfiguration` builds one `RaftPeer`/`AppendEntriesPeer`/`HeartbeatPeer` per *other*
  configured broker, over the real HTTP transport.
- **Majority is always computed from configured membership, never from reachable peers.**
  `ElectionCoordinator`/`RaftLogReplicator` are constructed with `clusterSize =
  clusterProperties.brokers().size()` - a broker that is down or partitioned is still a voting
  member, so a 3-broker cluster's majority stays 2 even while one broker is unreachable. It is never
  computed as `peers.size() + 1`, which would silently shrink under a failure - see
  `DockerBrokerClusterTest.quorumLossPreventsNewCommitsWithoutMisreadingReachablePeersAsTheWholeCluster`
  for the real, container-boundary test of exactly this.
- No dynamic reconfiguration (adding/removing voting members at runtime) is implemented.
- No Raft snapshotting or log compaction is implemented yet - the log grows unbounded.
- No external message broker (Kafka, RabbitMQ, Redis, etc.) is used anywhere - the queue engine and
  Raft implementation are entirely this project's own Java code.

## Running as Docker containers

### Building the image

```
docker build -t dmq-broker .
```

Multi-stage build (`Dockerfile`): a `gradle:9.7.1-jdk25` stage runs `gradle bootJar`, then the
runtime stage copies just the resulting jar onto `eclipse-temurin:25-jre`, running as a non-root
`broker` user. Using the official Gradle image (Gradle pre-installed) rather than this project's own
wrapper means the image build never depends on `services.gradle.org` being reachable - only on
Docker Hub, which any Docker build already depends on for its base images anyway.

Broker identity, cluster membership, and Raft/transport timing are **never baked into the image** -
they come entirely from Spring configuration (environment variables, relaxed-bound to the same
properties `application.yml` already defines) supplied at container run time. The same image runs
as any broker in the cluster; only its runtime configuration differs.

### Running a single container

```
docker run -p 8080:8080 \
  -e BROKER_ID=broker-1 -e BROKER_HOST=broker-1 -e BROKER_PORT=8080 -e SERVER_PORT=8080 \
  -e BROKER_DATA_DIRECTORY=/data/broker-1 \
  -e CLUSTER_BROKERS_0_ID=broker-1 -e CLUSTER_BROKERS_0_HOST=broker-1 -e CLUSTER_BROKERS_0_PORT=8080 \
  dmq-broker
```

`/data` is the image's writable data root (owned by the `broker` user) - point
`BROKER_DATA_DIRECTORY` at a path under it, or mount a volume over `/data` entirely, for data to
survive a container restart/recreation. `GET /actuator/health` is the readiness signal.

### The 3-node cluster

Each broker gets the *same* `CLUSTER_BROKERS_0/1/2_{ID,HOST,PORT}` env vars (relaxed-binding onto
`cluster.brokers[0..2]`), with `HOST` set to the other containers' **Docker network hostnames**
(their container/service name or network alias) - never `localhost`, which would only resolve to
the container itself. `clusterSize` is always 3 in this topology; majority is 2, computed from that
configured size regardless of how many brokers are actually reachable at any moment (see above).

```
docker network create dmq-net
docker run -d --name broker-1 --network dmq-net -e BROKER_ID=broker-1 -e BROKER_HOST=broker-1 ... dmq-broker
docker run -d --name broker-2 --network dmq-net -e BROKER_ID=broker-2 -e BROKER_HOST=broker-2 ... dmq-broker
docker run -d --name broker-3 --network dmq-net -e BROKER_ID=broker-3 -e BROKER_HOST=broker-3 ... dmq-broker
```

(A `docker-compose.yml` for manual local use may be added separately later; it is intentionally not
part of this milestone - Testcontainers owns the integration tests.)

### Testcontainers integration tests

`src/dockerTest` is a separate Gradle source set (its own task, `./gradlew dockerTest`; `check`
depends on it) from the fast `src/test` suite, since it needs a running Docker daemon and is
meaningfully slower - three real containers, a real Docker network, and real HTTP calls end to end.

`DockerBrokerCluster` (`src/dockerTest/.../docker/DockerBrokerCluster.java`) builds the real image
once per JVM (a plain `docker build`, not Testcontainers' own context-assembly - see its javadoc for
why), then starts N real containers on a real Testcontainers `Network`, with unique `BROKER_ID`s,
Docker-network-hostname aliases, and the same configured `CLUSTER_BROKERS_*` membership every
broker gets. All interaction from the test side goes over the real HTTP API - `StatusController`
for cluster/leader observability, `MessageController`/`ConsumerController` for publish/read/
fetch/commit - never Java internals. Readiness is `Wait.forHttp("/actuator/health")` with a bounded
startup timeout, not a sleep.

`stop()`/`restart()` use the raw Docker client (not `GenericContainer.stop()`, which is designed to
remove the container) so a "restart" is a genuine `docker stop` + `docker start` of the *same*
container - its writable filesystem, and therefore its data directory, survives, exactly like a real
broker process restart. (Docker Desktop for Windows was observed to assign a *different* random host
port on such a restart for a dynamically-published container port; `DockerBrokerCluster` re-inspects
and tracks the current mapping itself rather than trusting Testcontainers' cached value from the
original start - see its javadoc.)

`DockerBrokerClusterTest` covers: cluster formation with all three members configured and exactly
one leader elected; a real publish replicating and reaching queue state on every broker over HTTP;
leader failure electing a new leader while preserving previously-committed state; two-broker failure
correctly blocking new commits without misreading "one reachable node" as "a one-node cluster";
a stopped follower catching up automatically (no publish needed to trigger it) after restarting; an
old leader rejoining as a follower and converging without corrupting or duplicating committed state;
and consumer-group fetch/commit behaving at-least-once over the real HTTP API. `ContainerLogsOnFailureExtension`
prints each broker's captured container logs and last-known status only when a test actually fails.

**Deferred**: a dedicated named-volume persistence test. Every "restart" scenario above already
uses a genuine `docker stop`/`docker start` of the same container, which already exercises the real
WAL/Raft-log/term-vote/consumer-offset recovery-from-disk path (the container's writable layer is
untouched by stop/start) - a separate test recreating the container against an explicit mounted
volume was judged unnecessary additional complexity for this milestone.

## Observability

Micrometer (already pulled in by `spring-boot-starter-actuator`) plus
`io.micrometer:micrometer-registry-prometheus` exposes every broker's metrics in Prometheus format
at `/actuator/prometheus`. A small local stack - 3 brokers, Prometheus, Grafana, all on one Docker
Compose network - scrapes them and renders one Grafana dashboard.

**JMH vs. Prometheus/Grafana** - these answer different questions and are not a replacement for
each other: JMH (`src/jmh`, `./gradlew jmh`) is *controlled performance benchmarking* - an isolated
JVM, no network, no Raft, measuring one operation (e.g. `Partition.append()`) in a repeatable
environment. Prometheus/Grafana is *runtime system observability* - the real multi-broker cluster,
under real Raft/replication/HTTP traffic, over time. JMH tells you what one operation costs in
isolation; the dashboard tells you what the whole system is actually doing right now.

### Metrics added

All custom meters carry a `broker` tag (applied cluster-wide via `management.metrics.tags.broker`
in `application.yml`, so it also covers every JVM/HTTP metric Micrometer already provides, not just
the custom ones below).

| Area | Meters |
|---|---|
| Publish | `dmq_messages_published_total`, `dmq_publish_errors_total{reason}`, `dmq_publish_latency_seconds` (`Broker.publish()`, end-to-end including the Raft round trip) |
| WAL / persistence | `dmq_wal_append_total`, `dmq_wal_append_latency_seconds` (the real, `fsync`'d `Partition.append()`, timed from `RaftQueueStateMachine.apply()` - the only call site for a Raft-committed publish, run on the leader and every follower), `dmq_wal_errors_total` |
| Raft / replication | `dmq_raft_term`, `dmq_raft_commit_index`, `dmq_raft_last_applied_index`, `dmq_raft_state{raft_state}` (1/0 per state - the standard Prometheus pattern for an enum value), `dmq_replication_requests_total{peer,outcome}`, `dmq_replication_failures_total{peer}`, `dmq_replication_latency_seconds{peer}` (one leader-to-peer `AppendEntries` round trip) |
| Consumers | `dmq_consumer_fetch_total`, `dmq_consumer_commits_total`, `dmq_consumer_lag` (gauge, `Partition.nextOffset() - committedOffset`), `dmq_consumer_committed_offset` (gauge) - all tagged `consumer_group`, `topic`, `partition` |
| JVM / system / HTTP | Whatever Micrometer/Actuator already provides unmodified - `jvm_memory_used_bytes`, `jvm_gc_pause_seconds`, `jvm_threads_live_threads`, `process_cpu_usage`, `http_server_requests_seconds` (count/sum/max, tagged by URI/status/outcome) |

Every instrumented class keeps its original constructor unchanged and gained a new
`MeterRegistry`-accepting overload instead (defaulting to a throwaway `SimpleMeterRegistry` when
omitted) - existing tests that construct `Broker`, `RaftQueueStateMachine`, `RaftLogReplicator`, or
`ConsumerController` directly needed no changes at all; only the real Spring-wired beans (see
`RaftConfiguration`/`BrokerConfiguration`) pass the actual registry. `RaftNode` itself was not
touched - its Raft-state gauges are registered from a `MeterBinder` bean in `RaftConfiguration`
that only reads `RaftNode`'s existing getters.

### Running the stack

```
docker compose up --build
```

- Grafana: <http://localhost:3000> (anonymous admin access - local demo only, never appropriate
  outside it)
- Prometheus: <http://localhost:9090>
- Brokers: <http://localhost:8080> / `8081` / `8082`

The "DMQ Cluster" dashboard is provisioned automatically (`observability/grafana/provisioning`,
`observability/grafana/dashboards/dmq-cluster.json`) - open Grafana and it's already there, no
manual import. Prometheus scrapes `broker-1:8080` / `broker-2:8080` / `broker-3:8080` (Docker
service names on the Compose network - see `observability/prometheus/prometheus.yml`; never
`localhost`, which would only resolve to the Prometheus container itself).

**Before publishing**, create the same topic on all three brokers - topic creation is not yet
Raft-replicated (see Known limitations below), so a topic that only exists on the leader makes
every follower's replication fail with "unknown topic" once it tries to apply the committed
publish (a real, honestly-surfaced `dmq_replication_failures_total` increase, not a bug in the
metrics):

```
for p in 8080 8081 8082; do curl -X POST "http://localhost:$p/api/v1/topics?name=demo&partitions=1"; done
curl -X POST "http://localhost:8080/api/v1/messages?topic=demo&key=k1&payload=hello"
curl -X POST "http://localhost:8080/api/v1/consumer-groups/g1/fetch?topic=demo&partition=0"
curl -X POST "http://localhost:8080/api/v1/consumer-groups/g1/commit?topic=demo&partition=0&offset=1"
```

### What the dashboard demonstrates

- **Normal traffic / increased producer traffic** - Publish rate and Append latency (top row)
  respond immediately to publish volume.
- **Replication** - Replication activity/latency (middle row) shows every leader-to-peer
  `AppendEntries` round trip; WAL activity shows the same committed writes landing on followers a
  tick later.
- **Consumer lag** - `dmq_consumer_lag` rises the moment a publish outpaces a consumer group's
  commits, and falls back to 0 on the next commit.
- **Broker failure/restart** - stopping a broker container drops it out of Prometheus's `up` set
  and its `dmq_raft_state` series stops updating; the remaining brokers' Raft commit/last-applied
  indices keep advancing normally if a majority survives.
- **Raft leader change** - the "Broker Raft state" state-timeline panel and "Current leader" stat
  panel both flip to the newly elected broker the moment `dmq_raft_state{raft_state="LEADER"}`
  changes broker, with `dmq_raft_term` advancing alongside it.

### Known Windows/Docker limitation observed in this environment

Pulling the `prom/prometheus` and `grafana/grafana` images from Docker Hub was unreliable in the
sandboxed environment this was built in - `docker pull` repeatedly failed with a connection reset
against `auth.docker.io` specifically (`registry-1.docker.io` itself was reachable via plain
`curl`), succeeding only after several retries. This looks like sandbox-specific network
instability, not a problem with the Compose file or app configuration - the 3 broker images (built
locally from already-cached base images) started on the first attempt every time. If `docker
compose up` seems to hang on `prometheus`/`grafana` pulling, retry it; on a normal developer
machine with unrestricted Docker Hub access this is not expected to occur.

### Known limitations / next-milestone concerns

- **No PreVote extension.** A restarted node's own election timeout can fire before it hears from
  the cluster's current leader, incrementing its term and forcing a real step-down/re-election cycle
  even though its own (possibly stale) log means it can never actually win that election. The
  cluster always recovers - Raft's log-up-to-date voting rule guarantees a behind candidate cannot be
  elected - but this can cause a brief, legitimate leadership churn right after a former leader
  rejoins, occasionally producing one extra (still correctly committed, never corrupt) retried entry
  if a client happens to be publishing through the old leader at that exact moment. Adding a PreVote
  phase would remove this but is a change to the election algorithm itself, out of scope here.
- **A leader isolated from the majority does not step down on its own.** If a leader can no longer
  reach a majority of the configured cluster, `propose()` correctly throws
  `QuorumUnavailableException` and applies nothing, but the isolated node keeps calling itself
  `LEADER` locally until it is restarted or a real network partition heals.
- **Topic creation is not yet Raft-replicated** - only `PublishCommand`s are. Every node in a real
  multi-broker deployment needs identical topic configuration out of band until topic creation is
  proposed through Raft as well.
- **Testcontainers persistent-volume recovery is deferred** (see above) - covered indirectly by the
  same-container restart tests, not by a dedicated named-volume test.
- **No distributed tracing, alerting, or long-term metrics storage.** The Observability milestone
  covers metrics + one dashboard only, deliberately - see its own section above for what it does
  and does not cover.