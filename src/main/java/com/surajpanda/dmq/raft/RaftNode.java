package com.surajpanda.dmq.raft;

public class RaftNode {

  private final String nodeId;
  private long currentTerm;
  private RaftState state;
  private String votedFor;

  public RaftNode(String nodeId) {
    this.nodeId = nodeId;
    this.currentTerm = 0;
    this.state = RaftState.FOLLOWER;
    this.votedFor = null;
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

  public String getVotedFor() {
    return votedFor;
  }

  public void advanceTerm(long newTerm) {

    if (newTerm < currentTerm) {
      throw new IllegalArgumentException(
          "Cannot move to an older term: current=" + currentTerm + ", requested=" + newTerm);
    }

    if (newTerm > currentTerm) {
      currentTerm = newTerm;
      state = RaftState.FOLLOWER;
      votedFor = null;
    }
  }

  public void becomeCandidate() {

    if (state != RaftState.FOLLOWER) {
      throw new IllegalStateException("Only a FOLLOWER can become a CANDIDATE, was: " + state);
    }

    currentTerm++;
    state = RaftState.CANDIDATE;
    votedFor = nodeId;
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

  public RequestVoteResponse handleRequestVote(RequestVoteRequest request) {

    if (request.term() < currentTerm) {
      return new RequestVoteResponse(currentTerm, false);
    }

    if (request.term() > currentTerm) {
      advanceTerm(request.term());
    }

    boolean canGrantVote = votedFor == null || votedFor.equals(request.candidateId());

    if (!canGrantVote) {
      return new RequestVoteResponse(currentTerm, false);
    }

    votedFor = request.candidateId();
    return new RequestVoteResponse(currentTerm, true);
  }

  public HeartbeatResponse handleHeartbeat(Heartbeat heartbeat) {

    if (heartbeat.term() < currentTerm) {
      return new HeartbeatResponse(currentTerm, false);
    }

    if (heartbeat.term() > currentTerm) {
      advanceTerm(heartbeat.term());
    } else if (state == RaftState.CANDIDATE) {
      becomeFollower();
    }

    return new HeartbeatResponse(currentTerm, true);
  }
}
