package com.surajpanda.dmq.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class AppendEntriesTest {

  @Test
  void shouldRejectStaleLeaderTerm() {
    RaftNode node = new RaftNode("broker-1");
    node.advanceTerm(5);

    AppendEntriesResponse response =
        node.handleAppendEntries(new AppendEntriesRequest(3, "old-leader", 0, 0, List.of(), 0));

    assertFalse(response.success());
    assertEquals(5, response.term());
    assertEquals(RaftState.FOLLOWER, node.getState());
    assertEquals(0, node.getLog().lastIndex()); // untouched by a stale request
  }

  @Test
  void shouldUpdateTermAndBecomeFollowerOnNewerLeaderTerm() {
    RaftNode node = new RaftNode("broker-1");
    node.becomeCandidate();
    node.becomeLeader();

    AppendEntriesResponse response =
        node.handleAppendEntries(new AppendEntriesRequest(9, "leader-2", 0, 0, List.of(), 0));

    assertTrue(response.success());
    assertEquals(9, response.term());
    assertEquals(RaftState.FOLLOWER, node.getState());
    assertEquals(9, node.getCurrentTerm());
  }

  @Test
  void shouldStepDownSameTermCandidateOnAppendEntries() {
    RaftNode node = new RaftNode("broker-1");
    node.becomeCandidate();

    AppendEntriesResponse response =
        node.handleAppendEntries(new AppendEntriesRequest(1, "leader-1", 0, 0, List.of(), 0));

    assertTrue(response.success());
    assertEquals(RaftState.FOLLOWER, node.getState());
    assertEquals(1, node.getCurrentTerm());
  }

  @Test
  void shouldAppendEntriesInOrderAcrossSuccessiveRequests() {
    RaftNode node = new RaftNode("broker-1");

    AppendEntriesResponse first =
        node.handleAppendEntries(
            new AppendEntriesRequest(1, "leader-1", 0, 0, List.of(new LogEntry(1, 1, "cmd-1")), 0));
    AppendEntriesResponse second =
        node.handleAppendEntries(
            new AppendEntriesRequest(1, "leader-1", 1, 1, List.of(new LogEntry(1, 2, "cmd-2")), 0));

    assertTrue(first.success());
    assertTrue(second.success());
    assertEquals(2, node.getLog().lastIndex());
    assertEquals("cmd-1", node.getLog().get(1).orElseThrow().command());
    assertEquals("cmd-2", node.getLog().get(2).orElseThrow().command());
  }

  @Test
  void shouldRejectMismatchingPrevLogTerm() {
    RaftNode node = new RaftNode("broker-1");
    node.handleAppendEntries(
        new AppendEntriesRequest(1, "leader-1", 0, 0, List.of(new LogEntry(1, 1, "cmd-1")), 0));

    AppendEntriesResponse response =
        node.handleAppendEntries(
            new AppendEntriesRequest(
                2, "leader-2", 1, 99, List.of(new LogEntry(2, 2, "cmd-2")), 0));

    assertFalse(response.success());
    assertEquals(1, node.getLog().lastIndex()); // unchanged
  }

  @Test
  void shouldRejectMissingPrevLogIndex() {
    RaftNode node = new RaftNode("broker-1");

    AppendEntriesResponse response =
        node.handleAppendEntries(
            new AppendEntriesRequest(1, "leader-1", 5, 1, List.of(new LogEntry(1, 6, "cmd-6")), 0));

    assertFalse(response.success());
    assertEquals(0, node.getLog().lastIndex());
  }

  @Test
  void shouldNotDuplicateEntriesOnRetriedAppendEntries() {
    RaftNode node = new RaftNode("broker-1");
    AppendEntriesRequest request =
        new AppendEntriesRequest(1, "leader-1", 0, 0, List.of(new LogEntry(1, 1, "cmd-1")), 0);

    node.handleAppendEntries(request);
    AppendEntriesResponse retried = node.handleAppendEntries(request);

    assertTrue(retried.success());
    assertEquals(1, node.getLog().lastIndex());
  }

  @Test
  void shouldReplaceConflictingFollowerSuffix() {
    RaftNode node = new RaftNode("broker-1");
    node.handleAppendEntries(
        new AppendEntriesRequest(
            1,
            "leader-1",
            0,
            0,
            List.of(new LogEntry(1, 1, "cmd-1"), new LogEntry(1, 2, "stale-2")),
            0));

    AppendEntriesResponse response =
        node.handleAppendEntries(
            new AppendEntriesRequest(
                2, "leader-2", 1, 1, List.of(new LogEntry(2, 2, "cmd-2v2")), 0));

    assertTrue(response.success());
    assertEquals(2, node.getLog().lastIndex());
    assertEquals("cmd-2v2", node.getLog().get(2).orElseThrow().command());
  }

  @Test
  void leaderCommitIsCarriedInRequestEvenThoughUnusedThisCommit() {
    AppendEntriesRequest request = new AppendEntriesRequest(1, "leader-1", 0, 0, List.of(), 7);
    assertEquals(7, request.leaderCommit());
  }

  @Test
  void twoNodesMaintainIndependentLogs() {
    RaftNode nodeA = new RaftNode("broker-1");
    RaftNode nodeB = new RaftNode("broker-2");

    nodeA.handleAppendEntries(
        new AppendEntriesRequest(1, "leader-1", 0, 0, List.of(new LogEntry(1, 1, "cmd-1")), 0));

    assertEquals(1, nodeA.getLog().lastIndex());
    assertEquals(0, nodeB.getLog().lastIndex());
  }
}
