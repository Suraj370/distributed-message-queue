package com.surajpanda.dmq.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;

class RaftCommitIndexTest {

  @Test
  void commitIndexStartsAtZero() {
    RaftNode node = new RaftNode("broker-1");
    assertEquals(0, node.getCommitIndex());
  }

  @Test
  void commitIndexNeverMovesBackwards() {
    RaftNode node = new RaftNode("broker-1");
    node.getLog().appendCommand(1, "cmd-1");
    node.getLog().appendCommand(1, "cmd-2");

    node.advanceCommitIndex(2);
    node.advanceCommitIndex(1); // attempt to move backwards

    assertEquals(2, node.getCommitIndex());
  }

  @Test
  void followerCommitIndexCannotExceedItsOwnLog() {
    RaftNode node = new RaftNode("broker-1");
    node.getLog().appendCommand(1, "cmd-1");

    node.advanceCommitIndex(50); // leaderCommit far beyond this node's log

    assertEquals(1, node.getCommitIndex());
  }

  @Test
  void followerReceivesLeaderCommitThroughAppendEntries() {
    RaftNode follower = new RaftNode("broker-1");

    follower.handleAppendEntries(
        new AppendEntriesRequest(1, "leader-1", 0, 0, List.of(new LogEntry(1, 1, "cmd-1")), 1));

    assertEquals(1, follower.getCommitIndex());
  }

  @Test
  void followerCommitIndexBoundedByLeaderCommitEvenIfLogIsLonger() {
    RaftNode follower = new RaftNode("broker-1");

    follower.handleAppendEntries(
        new AppendEntriesRequest(
            1,
            "leader-1",
            0,
            0,
            List.of(new LogEntry(1, 1, "cmd-1"), new LogEntry(1, 2, "cmd-2")),
            1));

    assertEquals(1, follower.getCommitIndex()); // leaderCommit=1, even though log has 2 entries
  }

  @Test
  void singleNodeClusterCommitsItsOwnEntryWithoutPeers() {
    RaftNode leader = new RaftNode("broker-1");
    leader.becomeCandidate();
    leader.becomeLeader();
    leader.getLog().appendCommand(leader.getCurrentTerm(), "cmd-1");

    RaftLogReplicator replicator = new RaftLogReplicator(leader, List.of(), 1);
    replicator.initializeForNewLeader();
    replicator.replicate(leader.getCommitIndex());

    assertEquals(1, leader.getCommitIndex());
  }

  @Test
  void majorityReplicationAdvancesCommitIndexInThreeNodeCluster() {
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
    replicator.replicate(leader.getCommitIndex());

    // Leader (1) + both followers (2) = 3 of 3, well past the majority(2) threshold.
    assertEquals(1, leader.getCommitIndex());
  }

  @Test
  void threeNodeClusterCommitsWithLeaderAndOneSurvivingFollowerEvenThoughOneMemberIsUnreachable() {
    // The cluster is configured as 3 voting members, but only one follower is currently
    // reachable - the third member is omitted from the peer list entirely (down/partitioned),
    // not removed from the cluster. clusterSize must still be 3, and majority(3)=2 is met by the
    // leader plus this one surviving follower.
    RaftNode leader = new RaftNode("broker-1");
    leader.becomeCandidate();
    leader.becomeLeader();
    leader.getLog().appendCommand(leader.getCurrentTerm(), "cmd-1");

    RaftNode survivingFollower = new RaftNode("broker-2");
    List<AppendEntriesPeer> peers =
        List.of(
            new AppendEntriesPeer(
                "broker-2", new InProcessAppendEntriesConnection(survivingFollower)));

    RaftLogReplicator replicator = new RaftLogReplicator(leader, peers, 3);
    replicator.initializeForNewLeader();
    replicator.replicate(leader.getCommitIndex());

    assertEquals(1, leader.getCommitIndex());
  }

  @Test
  void threeNodeClusterLeaderAloneCannotCommitEvenWithNoReachablePeersListed() {
    // No peers are reachable at all, but the cluster is still configured as 3 voting members -
    // the leader's own single vote (1 of 3) must not be mistaken for a majority just because the
    // peer list it was given happens to be empty.
    RaftNode leader = new RaftNode("broker-1");
    leader.becomeCandidate();
    leader.becomeLeader();
    leader.getLog().appendCommand(leader.getCurrentTerm(), "cmd-1");

    RaftLogReplicator replicator = new RaftLogReplicator(leader, List.of(), 3);
    replicator.initializeForNewLeader();
    replicator.replicate(leader.getCommitIndex());

    assertEquals(0, leader.getCommitIndex());
  }

  @Test
  void fiveNodeClusterLeaderAndOneFollowerCannotCommit() {
    // clusterSize=5 -> majority=3. Leader + 1 reachable follower = 2 of 5, still a minority even
    // though the other 3 members are simply not in the reachable peer list.
    RaftNode leader = new RaftNode("broker-1");
    leader.becomeCandidate();
    leader.becomeLeader();
    leader.getLog().appendCommand(leader.getCurrentTerm(), "cmd-1");

    RaftNode follower = new RaftNode("broker-2");
    List<AppendEntriesPeer> peers =
        List.of(new AppendEntriesPeer("broker-2", new InProcessAppendEntriesConnection(follower)));

    RaftLogReplicator replicator = new RaftLogReplicator(leader, peers, 5);
    replicator.initializeForNewLeader();
    replicator.replicate(leader.getCommitIndex());

    assertEquals(0, leader.getCommitIndex());
  }

  @Test
  void fiveNodeClusterLeaderAndTwoFollowersCanCommit() {
    // clusterSize=5 -> majority=3. Leader + 2 reachable followers = 3 of 5, exactly the majority,
    // even though the other 2 configured members are unreachable and simply omitted.
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

    RaftLogReplicator replicator = new RaftLogReplicator(leader, peers, 5);
    replicator.initializeForNewLeader();
    replicator.replicate(leader.getCommitIndex());

    assertEquals(1, leader.getCommitIndex());
  }

  @Test
  void minorityReplicationDoesNotCommitInThreeNodeCluster() {
    RaftNode leader = new RaftNode("broker-1");
    leader.becomeCandidate();
    leader.becomeLeader();
    leader.getLog().appendCommand(leader.getCurrentTerm(), "cmd-1");

    // Both peers consistently reject (same term as the leader, so no step-down - just a log/peer
    // that never ends up accepting), leaving only the leader itself (1 of 3) with the entry -
    // below the majority(2) threshold.
    AppendEntriesConnection alwaysRejects =
        request -> new AppendEntriesResponse(request.term(), true, false, 0);

    List<AppendEntriesPeer> peers =
        List.of(
            new AppendEntriesPeer("broker-2", alwaysRejects),
            new AppendEntriesPeer("broker-3", alwaysRejects));

    RaftLogReplicator replicator = new RaftLogReplicator(leader, peers, 3);
    replicator.initializeForNewLeader();
    replicator.replicate(leader.getCommitIndex());

    assertEquals(RaftState.LEADER, leader.getState()); // no step-down; genuinely a minority
    assertEquals(0, leader.getCommitIndex());
  }

  @Test
  void onlyCurrentTermEntriesAreCommittedByTheMajorityRule() {
    // The leader carries one entry from an earlier term (already replicated to a majority in a
    // previous term) and has not yet appended anything in its own current term.
    RaftNode leader = new RaftNode("broker-1");
    leader.getLog().appendCommand(1, "old-term-entry");
    leader.becomeCandidate(); // term 1
    leader.becomeFollower();
    leader.becomeCandidate(); // term 2
    leader.becomeLeader();

    RaftNode follower = new RaftNode("broker-2");
    List<AppendEntriesPeer> peers =
        List.of(new AppendEntriesPeer("broker-2", new InProcessAppendEntriesConnection(follower)));

    RaftLogReplicator replicator = new RaftLogReplicator(leader, peers, 2);
    replicator.initializeForNewLeader();
    replicator.replicate(leader.getCommitIndex());

    // Both nodes have the entry, but its term (1) is not the leader's current term (2), so it
    // must not be newly committed by the majority rule alone.
    assertEquals(1, follower.getLog().lastIndex());
    assertEquals(0, leader.getCommitIndex());

    // Once a current-term entry also reaches a majority, it commits and carries the older entry
    // with it (commitIndex is a boundary, not a per-entry flag).
    leader.getLog().appendCommand(leader.getCurrentTerm(), "current-term-entry");
    replicator.replicate(leader.getCommitIndex());

    assertEquals(2, leader.getCommitIndex());
  }

  @Test
  void oldLeaderCannotCommitAfterSteppingDown() {
    RaftNode leader = new RaftNode("broker-1");
    leader.becomeCandidate();
    leader.becomeLeader();
    leader.getLog().appendCommand(leader.getCurrentTerm(), "cmd-1");

    RaftNode aheadPeer = new RaftNode("broker-2");
    aheadPeer.advanceTerm(9);
    List<AppendEntriesPeer> peers =
        List.of(new AppendEntriesPeer("broker-2", new InProcessAppendEntriesConnection(aheadPeer)));

    RaftLogReplicator replicator = new RaftLogReplicator(leader, peers, 2);
    replicator.initializeForNewLeader();
    replicator.replicate(leader.getCommitIndex()); // steps down on the peer's higher term

    assertEquals(RaftState.FOLLOWER, leader.getState());
    assertThrows(IllegalStateException.class, () -> replicator.replicate(leader.getCommitIndex()));
  }
}
