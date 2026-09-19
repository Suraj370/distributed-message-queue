package com.surajpanda.dmq.raft;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Orchestrates a single election attempt for a local RaftNode: transition to CANDIDATE, request
 * votes from peers in sequence, and become LEADER on a majority. Deliberately synchronous and
 * one-shot - no timers, no retries, no automatic re-election. Those belong to a later commit.
 *
 * <p>clusterSize is the configured voting membership of the whole cluster - deliberately separate
 * from peers.size(). peers is only the set of currently reachable RaftPeer connections; a peer that
 * is down or partitioned is simply omitted from that list rather than being removed from the
 * cluster (membership stays static in this milestone). Majority must be computed from clusterSize,
 * never from peers.size() + 1, or an unavailable member would silently shrink the majority
 * requirement and let a minority elect a leader.
 */
public class ElectionCoordinator {

  private final RaftNode localNode;
  private final List<RaftPeer> peers;
  private final int clusterSize;

  public ElectionCoordinator(RaftNode localNode, List<RaftPeer> peers, int clusterSize) {

    if (clusterSize < peers.size() + 1) {
      throw new IllegalArgumentException(
          "clusterSize must be at least the candidate plus every listed peer: clusterSize="
              + clusterSize
              + ", peers="
              + peers.size());
    }

    this.localNode = localNode;
    this.peers = List.copyOf(peers);
    this.clusterSize = clusterSize;
  }

  public boolean startElection() {

    localNode.becomeCandidate();

    long electionTerm = localNode.getCurrentTerm();

    ElectionTally tally =
        new ElectionTally(
            electionTerm,
            localNode.getNodeId(),
            peers.stream().map(RaftPeer::nodeId).collect(Collectors.toSet()));

    for (RaftPeer peer : peers) {

      if (localNode.getState() != RaftState.CANDIDATE
          || localNode.getCurrentTerm() != electionTerm) {
        break;
      }

      RequestVoteRequest request =
          new RequestVoteRequest(
              electionTerm,
              localNode.getNodeId(),
              localNode.getLog().lastIndex(),
              localNode.getLog().lastTerm());

      RequestVoteResponse response = peer.connection().requestVote(request);

      if (response.term() > localNode.getCurrentTerm()) {
        localNode.advanceTerm(response.term());
        break;
      }

      tally.recordResponse(peer.nodeId(), response);
    }

    if (localNode.getState() == RaftState.CANDIDATE
        && localNode.getCurrentTerm() == electionTerm
        && tally.hasMajority(clusterSize)) {
      localNode.becomeLeader();
      return true;
    }

    return false;
  }
}
