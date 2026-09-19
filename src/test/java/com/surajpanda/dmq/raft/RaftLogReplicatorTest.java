package com.surajpanda.dmq.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;

class RaftLogReplicatorTest {

  @Test
  void shouldRejectReplicatingWhenNotLeader() {
    RaftNode node = new RaftNode("broker-1");
    RaftLogReplicator replicator = new RaftLogReplicator(node, List.of());

    assertThrows(IllegalStateException.class, () -> replicator.replicate(0, 0, 0));
  }

  @Test
  void shouldReplicateMultipleEntriesInOrderToAFollower() {
    RaftNode leader = new RaftNode("broker-1");
    leader.becomeCandidate();
    leader.becomeLeader();
    leader.getLog().appendCommand(leader.getCurrentTerm(), "cmd-1");
    leader.getLog().appendCommand(leader.getCurrentTerm(), "cmd-2");
    leader.getLog().appendCommand(leader.getCurrentTerm(), "cmd-3");

    RaftNode follower = new RaftNode("broker-2");
    List<AppendEntriesPeer> peers =
        List.of(new AppendEntriesPeer("broker-2", new InProcessAppendEntriesConnection(follower)));

    new RaftLogReplicator(leader, peers).replicate(0, 0, 0);

    assertEquals(3, follower.getLog().lastIndex());
    assertEquals("cmd-1", follower.getLog().get(1).orElseThrow().command());
    assertEquals("cmd-2", follower.getLog().get(2).orElseThrow().command());
    assertEquals("cmd-3", follower.getLog().get(3).orElseThrow().command());
    assertEquals(RaftState.FOLLOWER, follower.getState());
    assertEquals(leader.getCurrentTerm(), follower.getCurrentTerm());
  }

  @Test
  void shouldReplicateToMultipleFollowersIndependently() {
    RaftNode leader = new RaftNode("broker-1");
    leader.becomeCandidate();
    leader.becomeLeader();
    leader.getLog().appendCommand(leader.getCurrentTerm(), "cmd-1");

    RaftNode follower1 = new RaftNode("broker-2");
    RaftNode follower2 = new RaftNode("broker-3");

    List<AppendEntriesPeer> peers =
        List.of(
            new AppendEntriesPeer("broker-2", new InProcessAppendEntriesConnection(follower1)),
            new AppendEntriesPeer("broker-3", new InProcessAppendEntriesConnection(follower2)));

    new RaftLogReplicator(leader, peers).replicate(0, 0, 0);

    assertEquals(1, follower1.getLog().lastIndex());
    assertEquals(1, follower2.getLog().lastIndex());
    assertEquals("cmd-1", follower1.getLog().get(1).orElseThrow().command());
    assertEquals("cmd-1", follower2.getLog().get(1).orElseThrow().command());
  }

  @Test
  void followerLogRemainsIndependentFromLeaderLogAfterReplication() {
    RaftNode leader = new RaftNode("broker-1");
    leader.becomeCandidate();
    leader.becomeLeader();
    leader.getLog().appendCommand(leader.getCurrentTerm(), "cmd-1");

    RaftNode follower = new RaftNode("broker-2");
    List<AppendEntriesPeer> peers =
        List.of(new AppendEntriesPeer("broker-2", new InProcessAppendEntriesConnection(follower)));
    new RaftLogReplicator(leader, peers).replicate(0, 0, 0);

    leader.getLog().appendCommand(leader.getCurrentTerm(), "cmd-2"); // leader keeps moving

    assertEquals(2, leader.getLog().lastIndex());
    assertEquals(1, follower.getLog().lastIndex()); // follower untouched by this later append
  }

  @Test
  void shouldStepDownWhenAPeerRespondsWithANewerTerm() {
    RaftNode leader = new RaftNode("broker-1");
    leader.becomeCandidate();
    leader.becomeLeader();

    RaftNode aheadPeer = new RaftNode("broker-2");
    aheadPeer.advanceTerm(9);

    List<AppendEntriesPeer> peers =
        List.of(new AppendEntriesPeer("broker-2", new InProcessAppendEntriesConnection(aheadPeer)));

    new RaftLogReplicator(leader, peers).replicate(0, 0, 0);

    assertEquals(RaftState.FOLLOWER, leader.getState());
    assertEquals(9, leader.getCurrentTerm());
  }
}
