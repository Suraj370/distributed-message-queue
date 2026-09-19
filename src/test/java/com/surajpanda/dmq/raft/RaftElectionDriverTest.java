package com.surajpanda.dmq.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

class RaftElectionDriverTest {

  private static RaftElectionDriver driverWithBusyPeer(
      RaftNode node, RaftNode peer, long startMillis) {
    List<RaftPeer> peers =
        List.of(new RaftPeer(peer.getNodeId(), new InProcessRaftPeerConnection(peer)));
    ElectionCoordinator coordinator = new ElectionCoordinator(node, peers, 2);
    ElectionTimeout timeout = new ElectionTimeout(100, 100, new Random(1));
    return new RaftElectionDriver(node, coordinator, timeout, startMillis);
  }

  @Test
  void shouldStartElectionAfterTimeoutElapses() {
    RaftNode node = new RaftNode("broker-1");
    RaftNode peer = new RaftNode("broker-2");
    peer.handleRequestVote(new RequestVoteRequest(1, "broker-9", 0, 0)); // peer busy, will reject

    RaftElectionDriver driver = driverWithBusyPeer(node, peer, 0);

    driver.tick(50);
    assertEquals(RaftState.FOLLOWER, node.getState());

    driver.tick(150);
    assertEquals(RaftState.CANDIDATE, node.getState());
    assertEquals(1, node.getCurrentTerm());
  }

  @Test
  void shouldNotStartElectionWhenValidHeartbeatsKeepArriving() {
    RaftNode node = new RaftNode("broker-1");
    RaftNode peer = new RaftNode("broker-2");
    peer.handleRequestVote(new RequestVoteRequest(1, "broker-9", 0, 0));

    RaftElectionDriver driver = driverWithBusyPeer(node, peer, 0);

    driver.onHeartbeatReceived(new Heartbeat(0, "leader-1"), 50);
    driver.tick(120);

    assertEquals(RaftState.FOLLOWER, node.getState());
  }

  @Test
  void heartbeatShouldResetTheElectionTimeout() {
    RaftNode node = new RaftNode("broker-1");
    RaftNode peer = new RaftNode("broker-2");
    peer.handleRequestVote(new RequestVoteRequest(1, "broker-9", 0, 0));

    RaftElectionDriver driver = driverWithBusyPeer(node, peer, 0);

    HeartbeatResponse response = driver.onHeartbeatReceived(new Heartbeat(0, "leader-1"), 90);
    assertTrue(response.accepted()); // pushes deadline to 190

    driver.tick(150); // would have elapsed at original deadline (100), but not the reset one

    assertEquals(RaftState.FOLLOWER, node.getState());
  }

  @Test
  void staleHeartbeatShouldNotResetTheElectionTimeout() {
    RaftNode node = new RaftNode("broker-1");
    node.advanceTerm(5);
    RaftNode peer = new RaftNode("broker-2");
    peer.handleRequestVote(
        new RequestVoteRequest(6, "broker-9", 0, 0)); // peer busy, will reject at term 6

    List<RaftPeer> peers = List.of(new RaftPeer("broker-2", new InProcessRaftPeerConnection(peer)));
    ElectionCoordinator coordinator = new ElectionCoordinator(node, peers, 2);
    ElectionTimeout timeout = new ElectionTimeout(100, 100, new Random(1));
    RaftElectionDriver driver = new RaftElectionDriver(node, coordinator, timeout, 0);

    HeartbeatResponse response = driver.onHeartbeatReceived(new Heartbeat(3, "old-leader"), 50);
    assertEquals(5, response.term());
    assertFalse(response.accepted());

    driver.tick(100); // original deadline reached; stale heartbeat must not have pushed it back

    assertEquals(RaftState.CANDIDATE, node.getState());
    assertEquals(6, node.getCurrentTerm());
  }

  @Test
  void candidateShouldStepDownToFollowerOnValidLeaderHeartbeat() {
    RaftNode node = new RaftNode("broker-1");
    node.becomeCandidate();

    ElectionTimeout timeout = new ElectionTimeout(100, 100, new Random(1));
    RaftElectionDriver driver =
        new RaftElectionDriver(node, new ElectionCoordinator(node, List.of(), 1), timeout, 0);

    HeartbeatResponse response = driver.onHeartbeatReceived(new Heartbeat(1, "leader-1"), 10);

    assertEquals(RaftState.FOLLOWER, node.getState());
    assertTrue(response.accepted());
  }

  @Test
  void leaderShouldNotStartElectionOnItsOwnTimeout() {
    RaftNode node = new RaftNode("broker-1");
    node.becomeCandidate();
    node.becomeLeader();

    ElectionTimeout timeout = new ElectionTimeout(100, 100, new Random(1));
    RaftElectionDriver driver =
        new RaftElectionDriver(node, new ElectionCoordinator(node, List.of(), 1), timeout, 0);

    driver.tick(10_000); // far past any timeout

    assertEquals(RaftState.LEADER, node.getState());
    assertEquals(1, node.getCurrentTerm());
  }
}
