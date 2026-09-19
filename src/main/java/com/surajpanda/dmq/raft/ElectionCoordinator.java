package com.surajpanda.dmq.raft;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Orchestrates a single election attempt for a local RaftNode: transition to CANDIDATE, request
 * votes from peers in sequence, and become LEADER on a majority. Deliberately synchronous and
 * one-shot - no timers, no retries, no automatic re-election. Those belong to a later commit.
 */
public class ElectionCoordinator {

  private final RaftNode localNode;
  private final List<RaftPeer> peers;

  public ElectionCoordinator(RaftNode localNode, List<RaftPeer> peers) {
    this.localNode = localNode;
    this.peers = List.copyOf(peers);
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

      RequestVoteResponse response =
          peer.connection()
              .requestVote(new RequestVoteRequest(electionTerm, localNode.getNodeId()));

      if (response.term() > localNode.getCurrentTerm()) {
        localNode.advanceTerm(response.term());
        break;
      }

      tally.recordResponse(peer.nodeId(), response);
    }

    int clusterSize = peers.size() + 1;

    if (localNode.getState() == RaftState.CANDIDATE
        && localNode.getCurrentTerm() == electionTerm
        && tally.hasMajority(clusterSize)) {
      localNode.becomeLeader();
      return true;
    }

    return false;
  }
}
