package com.surajpanda.dmq.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class ElectionCoordinatorTest {

  @Test
  void shouldBecomeLeaderInSingleNodeClusterFromSelfVote() {
    RaftNode candidate = new RaftNode("broker-1");
    ElectionCoordinator coordinator = new ElectionCoordinator(candidate, List.of(), 1);

    boolean elected = coordinator.startElection();

    assertTrue(elected);
    assertEquals(RaftState.LEADER, candidate.getState());
    assertEquals(1, candidate.getCurrentTerm());
  }

  @Test
  void shouldBecomeLeaderInThreeNodeClusterWithSelfAndOnePeerVote() {
    RaftNode candidate = new RaftNode("broker-1");
    RaftNode grantingPeer = new RaftNode("broker-2");
    RaftNode busyPeer = new RaftNode("broker-3");
    busyPeer.handleRequestVote(new RequestVoteRequest(1, "broker-9", 0, 0));

    List<RaftPeer> peers =
        List.of(
            new RaftPeer("broker-2", new InProcessRaftPeerConnection(grantingPeer)),
            new RaftPeer("broker-3", new InProcessRaftPeerConnection(busyPeer)));

    boolean elected = new ElectionCoordinator(candidate, peers, 3).startElection();

    assertTrue(elected);
    assertEquals(RaftState.LEADER, candidate.getState());
  }

  @Test
  void shouldNotBecomeLeaderInThreeNodeClusterWithOnlyOneVote() {
    RaftNode candidate = new RaftNode("broker-1");
    RaftNode busyPeer1 = new RaftNode("broker-2");
    RaftNode busyPeer2 = new RaftNode("broker-3");
    busyPeer1.handleRequestVote(new RequestVoteRequest(1, "broker-9", 0, 0));
    busyPeer2.handleRequestVote(new RequestVoteRequest(1, "broker-9", 0, 0));

    List<RaftPeer> peers =
        List.of(
            new RaftPeer("broker-2", new InProcessRaftPeerConnection(busyPeer1)),
            new RaftPeer("broker-3", new InProcessRaftPeerConnection(busyPeer2)));

    boolean elected = new ElectionCoordinator(candidate, peers, 3).startElection();

    assertFalse(elected);
    assertEquals(RaftState.CANDIDATE, candidate.getState());
  }

  @Test
  void shouldNotBecomeLeaderInFiveNodeClusterWithOnlyTwoVotes() {
    RaftNode candidate = new RaftNode("broker-1");
    RaftNode grantingPeer1 = new RaftNode("broker-2");
    RaftNode busyPeer1 = new RaftNode("broker-3");
    RaftNode busyPeer2 = new RaftNode("broker-4");
    RaftNode busyPeer3 = new RaftNode("broker-5");
    busyPeer1.handleRequestVote(new RequestVoteRequest(1, "broker-9", 0, 0));
    busyPeer2.handleRequestVote(new RequestVoteRequest(1, "broker-9", 0, 0));
    busyPeer3.handleRequestVote(new RequestVoteRequest(1, "broker-9", 0, 0));

    List<RaftPeer> peers =
        List.of(
            new RaftPeer("broker-2", new InProcessRaftPeerConnection(grantingPeer1)),
            new RaftPeer("broker-3", new InProcessRaftPeerConnection(busyPeer1)),
            new RaftPeer("broker-4", new InProcessRaftPeerConnection(busyPeer2)),
            new RaftPeer("broker-5", new InProcessRaftPeerConnection(busyPeer3)));

    boolean elected = new ElectionCoordinator(candidate, peers, 5).startElection();

    assertFalse(elected);
    assertEquals(RaftState.CANDIDATE, candidate.getState());
  }

  @Test
  void shouldBecomeLeaderInFiveNodeClusterWithSelfAndTwoPeerVotes() {
    RaftNode candidate = new RaftNode("broker-1");
    RaftNode grantingPeer1 = new RaftNode("broker-2");
    RaftNode grantingPeer2 = new RaftNode("broker-3");
    RaftNode busyPeer1 = new RaftNode("broker-4");
    RaftNode busyPeer2 = new RaftNode("broker-5");
    busyPeer1.handleRequestVote(new RequestVoteRequest(1, "broker-9", 0, 0));
    busyPeer2.handleRequestVote(new RequestVoteRequest(1, "broker-9", 0, 0));

    List<RaftPeer> peers =
        List.of(
            new RaftPeer("broker-2", new InProcessRaftPeerConnection(grantingPeer1)),
            new RaftPeer("broker-3", new InProcessRaftPeerConnection(grantingPeer2)),
            new RaftPeer("broker-4", new InProcessRaftPeerConnection(busyPeer1)),
            new RaftPeer("broker-5", new InProcessRaftPeerConnection(busyPeer2)));

    boolean elected = new ElectionCoordinator(candidate, peers, 5).startElection();

    assertTrue(elected);
    assertEquals(RaftState.LEADER, candidate.getState());
  }

  @Test
  void fiveNodeClusterWithOnlyOneReachablePeerCannotElectALeader() {
    // The cluster is configured as 5 voting members, but only one other member is currently
    // reachable - the other three are simply omitted from the peer list (down/partitioned), not
    // removed from the cluster. clusterSize must still be 5, so self + 1 reachable vote (2 of 5)
    // is below majority(5)=3, even though the reachable peer grants its vote.
    RaftNode candidate = new RaftNode("broker-1");
    RaftNode reachablePeer = new RaftNode("broker-2");

    List<RaftPeer> peers =
        List.of(new RaftPeer("broker-2", new InProcessRaftPeerConnection(reachablePeer)));

    boolean elected = new ElectionCoordinator(candidate, peers, 5).startElection();

    assertFalse(elected);
    assertEquals(RaftState.CANDIDATE, candidate.getState());
  }

  @Test
  void fiveNodeClusterWithTwoReachablePeersCanElectALeader() {
    // clusterSize=5 -> majority=3. Self + 2 reachable, granting peers = 3 of 5, meeting majority
    // even though the other 2 configured members are unreachable and simply omitted.
    RaftNode candidate = new RaftNode("broker-1");
    RaftNode reachablePeer1 = new RaftNode("broker-2");
    RaftNode reachablePeer2 = new RaftNode("broker-3");

    List<RaftPeer> peers =
        List.of(
            new RaftPeer("broker-2", new InProcessRaftPeerConnection(reachablePeer1)),
            new RaftPeer("broker-3", new InProcessRaftPeerConnection(reachablePeer2)));

    boolean elected = new ElectionCoordinator(candidate, peers, 5).startElection();

    assertTrue(elected);
    assertEquals(RaftState.LEADER, candidate.getState());
  }

  @Test
  void shouldStepDownToFollowerWhenPeerRespondsWithNewerTerm() {
    RaftNode candidate = new RaftNode("broker-1");
    RaftNode aheadPeer = new RaftNode("broker-2");
    aheadPeer.advanceTerm(5);

    List<RaftPeer> peers =
        List.of(new RaftPeer("broker-2", new InProcessRaftPeerConnection(aheadPeer)));

    boolean elected = new ElectionCoordinator(candidate, peers, 2).startElection();

    assertFalse(elected);
    assertEquals(RaftState.FOLLOWER, candidate.getState());
    assertEquals(5, candidate.getCurrentTerm());
  }

  @Test
  void shouldIgnoreStaleOlderTermResponseInsteadOfMiscountingIt() {
    RaftNode candidate = new RaftNode("broker-1");
    RaftPeerConnection staleConnection = request -> new RequestVoteResponse(0, true);

    List<RaftPeer> peers = List.of(new RaftPeer("broker-2", staleConnection));

    boolean elected = new ElectionCoordinator(candidate, peers, 2).startElection();

    assertFalse(elected);
    assertEquals(RaftState.CANDIDATE, candidate.getState());
    assertEquals(1, candidate.getCurrentTerm());
  }

  @Test
  void shouldRejectStartingAnElectionWhenAlreadyLeader() {
    RaftNode node = new RaftNode("broker-1");
    node.becomeCandidate();
    node.becomeLeader();

    ElectionCoordinator coordinator = new ElectionCoordinator(node, List.of(), 1);

    assertThrows(IllegalStateException.class, coordinator::startElection);
  }
}
