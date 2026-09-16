package com.surajpanda.dmq.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class RaftNodeTest {

  @Test
  void shouldStartAsFollower() {
    RaftNode node = new RaftNode("broker-1");
    assertEquals(RaftState.FOLLOWER, node.getState());
  }

  @Test
  void shouldStartAtTermZero() {
    RaftNode node = new RaftNode("broker-1");
    assertEquals(0, node.getCurrentTerm());
  }

  @Test
  void shouldAdvanceToANewerTerm() {
    RaftNode node = new RaftNode("broker-1");
    node.advanceTerm(5);
    assertEquals(5, node.getCurrentTerm());
  }

  @Test
  void shouldRejectMovingToAnOlderTerm() {
    RaftNode node = new RaftNode("broker-1");
    node.advanceTerm(5);
    assertThrows(IllegalArgumentException.class, () -> node.advanceTerm(3));
  }

  @Test
  void shouldNotChangeTermWhenAdvancingToTheSameTerm() {
    RaftNode node = new RaftNode("broker-1");
    node.advanceTerm(5);
    node.advanceTerm(5);
    assertEquals(5, node.getCurrentTerm());
  }

  @Test
  void shouldTransitionFromFollowerToCandidate() {
    RaftNode node = new RaftNode("broker-1");
    node.becomeCandidate();
    assertEquals(RaftState.CANDIDATE, node.getState());
  }

  @Test
  void shouldIncrementTermWhenBecomingCandidate() {
    RaftNode node = new RaftNode("broker-1");
    node.becomeCandidate();
    assertEquals(1, node.getCurrentTerm());
  }

  @Test
  void shouldTransitionFromCandidateToLeader() {
    RaftNode node = new RaftNode("broker-1");
    node.becomeCandidate();
    node.becomeLeader();
    assertEquals(RaftState.LEADER, node.getState());
  }

  @Test
  void shouldTransitionFromCandidateToFollower() {
    RaftNode node = new RaftNode("broker-1");
    node.becomeCandidate();
    node.becomeFollower();
    assertEquals(RaftState.FOLLOWER, node.getState());
  }

  @Test
  void shouldRejectBecomingCandidateWhenNotFollower() {
    RaftNode node = new RaftNode("broker-1");
    node.becomeCandidate();
    assertThrows(IllegalStateException.class, node::becomeCandidate);
  }

  @Test
  void shouldRejectBecomingLeaderWhenNotCandidate() {
    RaftNode node = new RaftNode("broker-1");
    assertThrows(IllegalStateException.class, node::becomeLeader);
  }

  @Test
  void shouldRejectVoluntaryFollowerStepDownWhenNotCandidate() {
    RaftNode node = new RaftNode("broker-1");
    assertThrows(IllegalStateException.class, node::becomeFollower);
  }

  @Test
  void shouldStepDownToFollowerWhenCandidateObservesNewerTerm() {
    RaftNode node = new RaftNode("broker-1");
    node.becomeCandidate();

    node.advanceTerm(5);

    assertEquals(RaftState.FOLLOWER, node.getState());
    assertEquals(5, node.getCurrentTerm());
  }

  @Test
  void shouldStepDownToFollowerWhenLeaderObservesNewerTerm() {
    RaftNode node = new RaftNode("broker-1");
    node.becomeCandidate();
    node.becomeLeader();

    node.advanceTerm(10);

    assertEquals(RaftState.FOLLOWER, node.getState());
    assertEquals(10, node.getCurrentTerm());
  }
}
