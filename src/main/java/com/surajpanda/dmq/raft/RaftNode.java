package com.surajpanda.dmq.raft;

public class RaftNode {

  private final String nodeId;
  private long currentTerm;
  private RaftState state;

  public RaftNode(String nodeId) {
    this.nodeId = nodeId;
    this.currentTerm = 0;
    this.state = RaftState.FOLLOWER;
  }

  public String getNodeId() {
    return nodeId;
  }

  public long getCurrentTerm() {
    return currentTerm;
  }

  public RaftState getState() {
    return state;
  }

  public void advanceTerm(long newTerm) {

    if (newTerm < currentTerm) {
      throw new IllegalArgumentException(
          "Cannot move to an older term: current=" + currentTerm + ", requested=" + newTerm);
    }

    if (newTerm > currentTerm) {
      currentTerm = newTerm;
    }
  }

  public void becomeCandidate() {

    if (state != RaftState.FOLLOWER) {
      throw new IllegalStateException("Only a FOLLOWER can become a CANDIDATE, was: " + state);
    }

    currentTerm++;
    state = RaftState.CANDIDATE;
  }

  public void becomeLeader() {

    if (state != RaftState.CANDIDATE) {
      throw new IllegalStateException("Only a CANDIDATE can become LEADER, was: " + state);
    }

    state = RaftState.LEADER;
  }

  public void becomeFollower() {

    if (state != RaftState.CANDIDATE) {
      throw new IllegalStateException(
          "Only a CANDIDATE can voluntarily step down to FOLLOWER, was: " + state);
    }

    state = RaftState.FOLLOWER;
  }
}
