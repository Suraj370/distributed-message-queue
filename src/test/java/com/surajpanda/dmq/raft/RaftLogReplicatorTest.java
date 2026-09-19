package com.surajpanda.dmq.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;

class RaftLogReplicatorTest {

  @Test
  void shouldRejectReplicatingWhenNotLeader() {
    RaftNode node = new RaftNode("broker-1");
    RaftLogReplicator replicator = new RaftLogReplicator(node, List.of(), 1);

    assertThrows(IllegalStateException.class, () -> replicator.replicate(0));
  }

  @Test
  void nextIndexShouldInitializeToLeaderLastIndexPlusOne() {
    RaftNode leader = new RaftNode("broker-1");
    leader.becomeCandidate();
    leader.becomeLeader();
    leader.getLog().appendCommand(leader.getCurrentTerm(), "cmd-1");
    leader.getLog().appendCommand(leader.getCurrentTerm(), "cmd-2");

    RaftLogReplicator replicator =
        new RaftLogReplicator(
            leader, List.of(new AppendEntriesPeer("broker-2", request -> null)), 2);
    replicator.initializeForNewLeader();

    assertEquals(3, replicator.getNextIndex("broker-2"));
  }

  @Test
  void matchIndexShouldInitializeToZero() {
    RaftNode leader = new RaftNode("broker-1");
    leader.becomeCandidate();
    leader.becomeLeader();
    leader.getLog().appendCommand(leader.getCurrentTerm(), "cmd-1");

    RaftLogReplicator replicator =
        new RaftLogReplicator(
            leader, List.of(new AppendEntriesPeer("broker-2", request -> null)), 2);
    replicator.initializeForNewLeader();

    assertEquals(0, replicator.getMatchIndex("broker-2"));
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
    RaftLogReplicator replicator = new RaftLogReplicator(leader, peers, 2);
    replicator.initializeForNewLeader();

    replicator.replicate(0);

    assertEquals(3, follower.getLog().lastIndex());
    assertEquals("cmd-1", follower.getLog().get(1).orElseThrow().command());
    assertEquals("cmd-2", follower.getLog().get(2).orElseThrow().command());
    assertEquals("cmd-3", follower.getLog().get(3).orElseThrow().command());
    assertEquals(RaftState.FOLLOWER, follower.getState());
    assertEquals(leader.getCurrentTerm(), follower.getCurrentTerm());
    assertEquals(3, replicator.getMatchIndex("broker-2"));
    assertEquals(4, replicator.getNextIndex("broker-2"));
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
    RaftLogReplicator replicator = new RaftLogReplicator(leader, peers, 3);
    replicator.initializeForNewLeader();

    replicator.replicate(0);

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
    RaftLogReplicator replicator = new RaftLogReplicator(leader, peers, 2);
    replicator.initializeForNewLeader();
    replicator.replicate(0);

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
    RaftLogReplicator replicator = new RaftLogReplicator(leader, peers, 2);
    replicator.initializeForNewLeader();

    replicator.replicate(0);

    assertEquals(RaftState.FOLLOWER, leader.getState());
    assertEquals(9, leader.getCurrentTerm());
  }

  @Test
  void shouldBacktrackNextIndexAndReplaceConflictingFollowerSuffix() {
    // Follower has a stale suffix from a previous (lower) term at index 2.
    RaftNode follower = new RaftNode("broker-2");
    follower
        .getLog()
        .appendEntries(0, 0, List.of(new LogEntry(1, 1, "cmd-1"), new LogEntry(1, 2, "stale-2")));

    // Leader carries the same index-1 entry, but a different (newer-term) index 2 and 3.
    RaftNode leader = new RaftNode("broker-1");
    leader.getLog().appendCommand(1, "cmd-1");
    leader.becomeCandidate(); // term 1
    leader.becomeFollower();
    leader.becomeCandidate(); // term 2
    leader.becomeLeader();
    leader.getLog().appendCommand(2, "cmd-2-v2");
    leader.getLog().appendCommand(2, "cmd-3");

    List<AppendEntriesPeer> peers =
        List.of(new AppendEntriesPeer("broker-2", new InProcessAppendEntriesConnection(follower)));
    RaftLogReplicator replicator = new RaftLogReplicator(leader, peers, 2);
    replicator.initializeForNewLeader();
    assertEquals(4, replicator.getNextIndex("broker-2")); // leader lastIndex(3) + 1

    replicator.replicate(0);

    // Backtracked from nextIndex 4 -> 3 -> 2 before finding the matching prefix at index 1.
    assertEquals(3, follower.getLog().lastIndex());
    assertEquals("cmd-1", follower.getLog().get(1).orElseThrow().command());
    assertEquals("cmd-2-v2", follower.getLog().get(2).orElseThrow().command());
    assertEquals(2, follower.getLog().get(2).orElseThrow().term());
    assertEquals("cmd-3", follower.getLog().get(3).orElseThrow().command());
    assertEquals(3, replicator.getMatchIndex("broker-2"));
    assertEquals(4, replicator.getNextIndex("broker-2"));
  }
}
