package com.surajpanda.dmq.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

class RaftElectionDriverAppendEntriesTest {

  @Test
  void successfulAppendEntriesResetsTheElectionTimeout() {
    RaftNode node = new RaftNode("broker-1");
    RaftNode busyPeer = new RaftNode("broker-2");
    busyPeer.handleRequestVote(new RequestVoteRequest(1, "broker-9"));

    List<RaftPeer> peers =
        List.of(new RaftPeer("broker-2", new InProcessRaftPeerConnection(busyPeer)));
    ElectionCoordinator coordinator = new ElectionCoordinator(node, peers);
    ElectionTimeout timeout = new ElectionTimeout(100, 100, new Random(1));
    RaftElectionDriver driver = new RaftElectionDriver(node, coordinator, timeout, 0);

    AppendEntriesResponse response =
        driver.onAppendEntriesReceived(
            new AppendEntriesRequest(0, "leader-1", 0, 0, List.of(), 0), 90);

    assertTrue(response.termAccepted());
    assertTrue(response.success());

    driver.tick(150); // would have elapsed at the original deadline (100), but not the reset one

    assertEquals(RaftState.FOLLOWER, node.getState());
  }

  @Test
  void currentTermAppendEntriesWithLogMismatchStillResetsTheElectionTimeout() {
    RaftNode node = new RaftNode("broker-1");
    RaftNode busyPeer = new RaftNode("broker-2");
    busyPeer.handleRequestVote(new RequestVoteRequest(1, "broker-9"));

    List<RaftPeer> peers =
        List.of(new RaftPeer("broker-2", new InProcessRaftPeerConnection(busyPeer)));
    ElectionCoordinator coordinator = new ElectionCoordinator(node, peers);
    ElectionTimeout timeout = new ElectionTimeout(100, 100, new Random(1));
    RaftElectionDriver driver = new RaftElectionDriver(node, coordinator, timeout, 0);

    // Log mismatch: prevLogIndex 5 on an empty follower log, but the term (0) is current.
    AppendEntriesResponse response =
        driver.onAppendEntriesReceived(
            new AppendEntriesRequest(0, "leader-1", 5, 1, List.of(), 0), 90);

    assertTrue(response.termAccepted()); // the leader is legitimate
    assertFalse(response.success()); // but the log didn't match

    driver.tick(150); // would have elapsed at the original deadline (100), but the timeout reset

    assertEquals(RaftState.FOLLOWER, node.getState()); // no election started
  }

  @Test
  void staleTermAppendEntriesDoesNotResetTheElectionTimeout() {
    RaftNode node = new RaftNode("broker-1");
    node.advanceTerm(5);
    RaftNode busyPeer = new RaftNode("broker-2");
    busyPeer.handleRequestVote(new RequestVoteRequest(6, "broker-9")); // will reject at term 6

    List<RaftPeer> peers =
        List.of(new RaftPeer("broker-2", new InProcessRaftPeerConnection(busyPeer)));
    ElectionCoordinator coordinator = new ElectionCoordinator(node, peers);
    ElectionTimeout timeout = new ElectionTimeout(100, 100, new Random(1));
    RaftElectionDriver driver = new RaftElectionDriver(node, coordinator, timeout, 0);

    AppendEntriesResponse response =
        driver.onAppendEntriesReceived(
            new AppendEntriesRequest(3, "old-leader", 0, 0, List.of(), 0), 50);

    assertFalse(response.termAccepted());
    assertFalse(response.success());
    assertEquals(5, response.term());

    driver.tick(100); // original deadline reached; stale request must not have pushed it back

    assertEquals(RaftState.CANDIDATE, node.getState());
    assertEquals(6, node.getCurrentTerm());
  }

  @Test
  void newerTermAppendEntriesWithLogMismatchUpdatesStateAndResetsTheElectionTimeout() {
    RaftNode node = new RaftNode("broker-1");
    RaftNode busyPeer = new RaftNode("broker-2");
    busyPeer.handleRequestVote(new RequestVoteRequest(10, "broker-9"));

    List<RaftPeer> peers =
        List.of(new RaftPeer("broker-2", new InProcessRaftPeerConnection(busyPeer)));
    ElectionCoordinator coordinator = new ElectionCoordinator(node, peers);
    ElectionTimeout timeout = new ElectionTimeout(100, 100, new Random(1));
    RaftElectionDriver driver = new RaftElectionDriver(node, coordinator, timeout, 0);

    // Newer term (9), but a log mismatch (prevLogIndex 5 on an empty follower log).
    AppendEntriesResponse response =
        driver.onAppendEntriesReceived(
            new AppendEntriesRequest(9, "leader-2", 5, 1, List.of(), 0), 90);

    assertTrue(response.termAccepted());
    assertFalse(response.success());
    assertEquals(9, node.getCurrentTerm());
    assertEquals(RaftState.FOLLOWER, node.getState());

    driver.tick(150); // would have elapsed at the original deadline (100), but the timeout reset

    assertEquals(RaftState.FOLLOWER, node.getState()); // no election started
    assertEquals(9, node.getCurrentTerm());
  }

  @Test
  void candidateStepsDownToFollowerOnValidAppendEntries() {
    RaftNode node = new RaftNode("broker-1");
    node.becomeCandidate();

    ElectionTimeout timeout = new ElectionTimeout(100, 100, new Random(1));
    RaftElectionDriver driver =
        new RaftElectionDriver(node, new ElectionCoordinator(node, List.of()), timeout, 0);

    AppendEntriesResponse response =
        driver.onAppendEntriesReceived(
            new AppendEntriesRequest(1, "leader-1", 0, 0, List.of(), 0), 10);

    assertEquals(RaftState.FOLLOWER, node.getState());
    assertTrue(response.termAccepted());
    assertTrue(response.success());
  }
}
