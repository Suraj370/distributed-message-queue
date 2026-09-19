package com.surajpanda.dmq.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class HeartbeatTest {

  @Test
  void shouldUpdateTermAndStayFollowerOnNewerTermHeartbeat() {
    RaftNode node = new RaftNode("broker-1");

    HeartbeatResponse response = node.handleHeartbeat(new Heartbeat(5, "leader-1"));

    assertEquals(5, node.getCurrentTerm());
    assertEquals(RaftState.FOLLOWER, node.getState());
    assertEquals(5, response.term());
  }

  @Test
  void shouldStepDownCandidateOnValidSameTermHeartbeat() {
    RaftNode node = new RaftNode("broker-1");
    node.becomeCandidate();

    HeartbeatResponse response = node.handleHeartbeat(new Heartbeat(1, "leader-1"));

    assertEquals(RaftState.FOLLOWER, node.getState());
    assertEquals(1, node.getCurrentTerm());
    assertEquals(1, response.term());
  }

  @Test
  void shouldRejectStaleTermHeartbeatWithoutChangingState() {
    RaftNode node = new RaftNode("broker-1");
    node.advanceTerm(5);

    HeartbeatResponse response = node.handleHeartbeat(new Heartbeat(3, "old-leader"));

    assertEquals(5, node.getCurrentTerm());
    assertEquals(RaftState.FOLLOWER, node.getState());
    assertEquals(5, response.term());
  }

  @Test
  void shouldNotStepDownLeaderOnStaleTermHeartbeat() {
    RaftNode node = new RaftNode("broker-1");
    node.becomeCandidate();
    node.becomeLeader();

    HeartbeatResponse response = node.handleHeartbeat(new Heartbeat(0, "stale-leader"));

    assertEquals(RaftState.LEADER, node.getState());
    assertEquals(1, node.getCurrentTerm());
    assertEquals(1, response.term());
  }

  @Test
  void shouldStaySameFollowerOnSameTermHeartbeat() {
    RaftNode node = new RaftNode("broker-1");

    HeartbeatResponse response = node.handleHeartbeat(new Heartbeat(0, "leader-1"));

    assertEquals(RaftState.FOLLOWER, node.getState());
    assertEquals(0, response.term());
  }
}
