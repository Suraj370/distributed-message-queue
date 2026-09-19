package com.surajpanda.dmq.raft;

public class RaftNode {

  private final String nodeId;
  private long currentTerm;
  private RaftState state;
  private String votedFor;
  private final RaftLog log = new RaftLog();
  private long commitIndex = 0;
  private long lastApplied = 0;

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

  public RaftLog getLog() {
    return log;
  }

  public long getCommitIndex() {
    return commitIndex;
  }

  public long getLastApplied() {
    return lastApplied;
  }

  /**
   * Advances commitIndex to the given value, clamped to this node's own log length (a follower's
   * commitIndex must never claim entries it hasn't actually stored) and never moving backwards. A
   * candidateCommitIndex at or below the current commitIndex, or beyond the local log, is simply a
   * no-op rather than an error - both a leader replaying an old leaderCommit value and a follower
   * temporarily behind on its log are normal, expected situations, not caller mistakes.
   */
  public void advanceCommitIndex(long candidateCommitIndex) {
    long bounded = Math.min(candidateCommitIndex, log.lastIndex());
    if (bounded > commitIndex) {
      commitIndex = bounded;
    }
  }

  /**
   * Marks entries up to and including index as applied to the state machine. Throws if asked to
   * apply beyond commitIndex - lastApplied must only ever advance through already-committed
   * entries, never ahead of them.
   */
  public void markApplied(long index) {
    if (index > commitIndex) {
      throw new IllegalArgumentException(
          "Cannot mark applied beyond commitIndex: index="
              + index
              + ", commitIndex="
              + commitIndex);
    }
    if (index > lastApplied) {
      lastApplied = index;
    }
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

  public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {

    if (request.term() < currentTerm) {
      return new AppendEntriesResponse(currentTerm, false, false, log.lastIndex());
    }

    if (request.term() > currentTerm) {
      advanceTerm(request.term());
    } else if (state == RaftState.CANDIDATE) {
      becomeFollower();
    }

    boolean matched =
        log.appendEntries(request.prevLogIndex(), request.prevLogTerm(), request.entries());

    if (matched) {
      advanceCommitIndex(request.leaderCommit());
    }

    return new AppendEntriesResponse(currentTerm, true, matched, log.lastIndex());
  }
}
