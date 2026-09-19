package com.surajpanda.dmq.raft;

import java.util.List;

/**
 * Minimal leader-side AppendEntries sender: replicates a caller-specified slice of the leader's log
 * to each peer in one synchronous round, mirroring HeartbeatBroadcaster's single-attempt shape.
 * Per-peer nextIndex/matchIndex tracking, retry-on-mismatch, and majority-commit advancement are
 * deferred to Commit 6 - this only proves out the request/response wiring and the receiving side's
 * log-consistency handling.
 */
public class RaftLogReplicator {

  private final RaftNode localNode;
  private final List<AppendEntriesPeer> peers;

  public RaftLogReplicator(RaftNode localNode, List<AppendEntriesPeer> peers) {
    this.localNode = localNode;
    this.peers = List.copyOf(peers);
  }

  /**
   * Sends every entry after (prevLogIndex, prevLogTerm) through the end of the leader's log to each
   * peer. Does not retry, backtrack on rejection, or track per-peer progress.
   */
  public void replicate(long prevLogIndex, long prevLogTerm, long leaderCommit) {

    if (localNode.getState() != RaftState.LEADER) {
      throw new IllegalStateException(
          "Only a LEADER replicates its log, was: " + localNode.getState());
    }

    long term = localNode.getCurrentTerm();
    List<LogEntry> entries = localNode.getLog().entriesFrom(prevLogIndex + 1);

    for (AppendEntriesPeer peer : peers) {

      if (localNode.getState() != RaftState.LEADER) {
        break;
      }

      AppendEntriesRequest request =
          new AppendEntriesRequest(
              term, localNode.getNodeId(), prevLogIndex, prevLogTerm, entries, leaderCommit);

      AppendEntriesResponse response = peer.connection().sendAppendEntries(request);

      if (response.term() > localNode.getCurrentTerm()) {
        localNode.advanceTerm(response.term());
      }
    }
  }
}
