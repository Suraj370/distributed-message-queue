package com.surajpanda.dmq.raft;

/**
 * Deterministic, time-injected driver tying an ElectionTimeout to an ElectionCoordinator for one
 * RaftNode. Owns no thread and no clock - a test (or a real scheduler) decides when time has
 * "moved" by calling tick(nowMillis), which keeps election-timeout behavior fully unit-testable
 * without waiting on a wall clock.
 */
public class RaftElectionDriver {

  private final RaftNode node;
  private final ElectionCoordinator coordinator;
  private final ElectionTimeout electionTimeout;

  public RaftElectionDriver(
      RaftNode node,
      ElectionCoordinator coordinator,
      ElectionTimeout electionTimeout,
      long startMillis) {
    this.node = node;
    this.coordinator = coordinator;
    this.electionTimeout = electionTimeout;
    electionTimeout.reset(startMillis);
  }

  /**
   * Called periodically with the current time. A LEADER never times out here. A FOLLOWER or
   * CANDIDATE whose timeout has elapsed starts a fresh election; a CANDIDATE first steps back to
   * FOLLOWER so it can legally re-enter becomeCandidate() with a new term, which is what "a
   * candidate restarts its own election on timeout" means in this implementation.
   */
  public void tick(long nowMillis) {

    RaftState state = node.getState();

    if (state == RaftState.LEADER) {
      return;
    }

    if (!electionTimeout.hasElapsed(nowMillis)) {
      return;
    }

    if (state == RaftState.CANDIDATE) {
      node.becomeFollower();
    }

    coordinator.startElection();
    electionTimeout.reset(nowMillis);
  }

  /**
   * Called whenever a heartbeat is received. Delegates the Raft state rules to
   * RaftNode.handleHeartbeat, and only resets the election timeout when the heartbeat was actually
   * accepted - a rejected/stale heartbeat must not extend the timeout.
   */
  public HeartbeatResponse onHeartbeatReceived(Heartbeat heartbeat, long nowMillis) {

    HeartbeatResponse response = node.handleHeartbeat(heartbeat);

    if (response.accepted()) {
      electionTimeout.reset(nowMillis);
    }

    return response;
  }

  /**
   * Called whenever an AppendEntries request is received. Reuses the same ElectionTimeout as
   * onHeartbeatReceived - there is only ever one timeout mechanism per node. The timeout is reset
   * whenever the request came from a current/newer-term leader (response.termAccepted()),
   * regardless of whether the log-matching check (response.success()) passed - a legitimate, alive
   * leader must not be mistaken for absent just because a particular AppendEntries hit a log
   * mismatch. Only a stale-term request (termAccepted() == false) leaves the timeout untouched.
   */
  public AppendEntriesResponse onAppendEntriesReceived(
      AppendEntriesRequest request, long nowMillis) {

    AppendEntriesResponse response = node.handleAppendEntries(request);

    if (response.termAccepted()) {
      electionTimeout.reset(nowMillis);
    }

    return response;
  }
}
