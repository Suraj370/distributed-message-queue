package com.surajpanda.dmq.raft;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Leader-side replication: sends AppendEntries to each peer, tracking per-peer nextIndex and
 * matchIndex, and backtracking nextIndex on a log mismatch until the peer accepts a matching
 * prefix. Because prevLogIndex 0 always matches (the Raft "before the log" sentinel), nextIndex
 * strictly decreases on every rejection and the retry loop is guaranteed to terminate within at
 * most the peer's starting nextIndex attempts - no unbounded retry is possible.
 *
 * <p>After a replication round, attempts to advance the leader's own commitIndex using the
 * majority-replication rule, restricted to entries from the leader's current term (an older-term
 * entry only becomes committed indirectly, by a later current-term entry committing over it - the
 * standard Raft commit-safety rule).
 *
 * <p>initializeForNewLeader() must be called once when this node becomes LEADER, before the first
 * replicate() call: nextIndex resets to lastLogIndex+1 and matchIndex resets to 0 for every peer on
 * each new leadership term, per Raft §5.3.
 *
 * <p>clusterSize is the configured voting membership of the whole cluster - it is deliberately a
 * separate number from peers.size(). peers is only the set of currently reachable AppendEntries
 * connections; a peer that is down or partitioned is simply omitted from that list rather than
 * being removed from the cluster (membership stays static in this milestone - see requirement 5).
 * Majority must be computed from clusterSize, never from peers.size() + 1, or an unavailable member
 * would silently shrink the majority requirement and let a minority commit.
 *
 * <p>Thread safety: nextIndex/matchIndex are plain HashMaps mutated by replicate(), read by
 * getNextIndex()/getMatchIndex(), and reset by initializeForNewLeader() - all four are synchronized
 * on this instance. This matters because replicate() is called from more than one thread in
 * production: a client-request thread (via RaftReplicatedQueue.propose()) and the periodic
 * scheduler thread (via RaftNodeScheduler's onTick hook, driving catch-up replication even without
 * a new publish). Serializing those calls keeps the per-peer bookkeeping consistent without
 * changing any of the Raft decision logic itself.
 */
public class RaftLogReplicator {

  private final RaftNode localNode;
  private final List<AppendEntriesPeer> peers;
  private final int clusterSize;
  private final MeterRegistry meterRegistry;
  private final Map<String, Long> nextIndex = new HashMap<>();
  private final Map<String, Long> matchIndex = new HashMap<>();

  public RaftLogReplicator(RaftNode localNode, List<AppendEntriesPeer> peers, int clusterSize) {
    this(localNode, peers, clusterSize, new SimpleMeterRegistry());
  }

  /**
   * Same as the three-arg constructor, but records replication activity/latency/failures into
   * meterRegistry (see {@link #replicateToPeer}). The three-arg constructor defaults to a throwaway
   * {@link SimpleMeterRegistry} so every existing caller (tests included) keeps working exactly as
   * before; only the real Spring-wired broker (see RaftConfiguration) needs the metrics to actually
   * go anywhere.
   */
  public RaftLogReplicator(
      RaftNode localNode,
      List<AppendEntriesPeer> peers,
      int clusterSize,
      MeterRegistry meterRegistry) {

    if (clusterSize < peers.size() + 1) {
      throw new IllegalArgumentException(
          "clusterSize must be at least the leader plus every listed peer: clusterSize="
              + clusterSize
              + ", peers="
              + peers.size());
    }

    this.localNode = localNode;
    this.peers = List.copyOf(peers);
    this.clusterSize = clusterSize;
    this.meterRegistry = meterRegistry;
  }

  public synchronized void initializeForNewLeader() {
    long lastIndex = localNode.getLog().lastIndex();
    for (AppendEntriesPeer peer : peers) {
      nextIndex.put(peer.nodeId(), lastIndex + 1);
      matchIndex.put(peer.nodeId(), 0L);
    }
  }

  public synchronized long getNextIndex(String peerId) {
    return nextIndex.getOrDefault(peerId, localNode.getLog().lastIndex() + 1);
  }

  public synchronized long getMatchIndex(String peerId) {
    return matchIndex.getOrDefault(peerId, 0L);
  }

  /** Replicates to every peer, then attempts to advance the leader's commitIndex. */
  public synchronized void replicate(long leaderCommit) {

    if (localNode.getState() != RaftState.LEADER) {
      throw new IllegalStateException(
          "Only a LEADER replicates its log, was: " + localNode.getState());
    }

    for (AppendEntriesPeer peer : peers) {
      if (localNode.getState() != RaftState.LEADER) {
        break;
      }
      replicateToPeer(peer, leaderCommit);
    }

    tryAdvanceCommitIndex();
  }

  private void replicateToPeer(AppendEntriesPeer peer, long leaderCommit) {

    while (localNode.getState() == RaftState.LEADER) {

      long peerNextIndex = getNextIndex(peer.nodeId());
      long prevLogIndex = peerNextIndex - 1;
      long prevLogTerm =
          prevLogIndex == 0
              ? 0
              : localNode.getLog().get(prevLogIndex).map(LogEntry::term).orElse(0L);
      List<LogEntry> entries = localNode.getLog().entriesFrom(peerNextIndex);

      AppendEntriesRequest request =
          new AppendEntriesRequest(
              localNode.getCurrentTerm(),
              localNode.getNodeId(),
              prevLogIndex,
              prevLogTerm,
              entries,
              leaderCommit);

      Timer.Sample replicationTiming = Timer.start(meterRegistry);
      AppendEntriesResponse response = peer.connection().sendAppendEntries(request);
      replicationTiming.stop(
          Timer.builder("dmq_replication_latency_seconds")
              .description("Latency of one leader-to-peer AppendEntries round trip")
              .tag("peer", peer.nodeId())
              .register(meterRegistry));
      meterRegistry
          .counter(
              "dmq_replication_requests_total",
              "peer",
              peer.nodeId(),
              "outcome",
              response.success() ? "success" : "failure")
          .increment();
      if (!response.success()) {
        meterRegistry.counter("dmq_replication_failures_total", "peer", peer.nodeId()).increment();
      }

      if (response.term() > localNode.getCurrentTerm()) {
        localNode.advanceTerm(response.term());
        return;
      }

      if (response.success()) {
        long newMatchIndex = prevLogIndex + entries.size();
        matchIndex.put(peer.nodeId(), newMatchIndex);
        nextIndex.put(peer.nodeId(), newMatchIndex + 1);
        return;
      }

      if (peerNextIndex <= 1) {
        // prevLogIndex 0 always matches, so rejection here means a stale/invalid response rather
        // than a real log mismatch - stop instead of retrying forever.
        return;
      }

      nextIndex.put(peer.nodeId(), peerNextIndex - 1);
    }
  }

  private void tryAdvanceCommitIndex() {

    if (localNode.getState() != RaftState.LEADER) {
      return;
    }

    long currentTerm = localNode.getCurrentTerm();
    int majority = (clusterSize / 2) + 1;

    for (long candidate = localNode.getLog().lastIndex();
        candidate > localNode.getCommitIndex();
        candidate--) {

      long entryTerm = localNode.getLog().get(candidate).map(LogEntry::term).orElse(-1L);
      if (entryTerm != currentTerm) {
        continue; // only a current-term entry can be newly committed by the majority rule
      }

      int replicatedCount = 1; // the leader's own log counts toward its own matchIndex
      for (AppendEntriesPeer peer : peers) {
        if (getMatchIndex(peer.nodeId()) >= candidate) {
          replicatedCount++;
        }
      }

      if (replicatedCount >= majority) {
        localNode.advanceCommitIndex(candidate);
        return;
      }
    }
  }
}
