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
}
