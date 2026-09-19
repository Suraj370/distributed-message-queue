package com.surajpanda.dmq.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * End-to-end, deterministic simulation of leader failover in a three-node cluster, reusing the same
 * election-timeout/election machinery from earlier commits rather than any new mechanism. "A
 * becomes unavailable" is modeled by simply no longer invoking anything on A and, for B's
 * re-election, wiring B's connection to A as one that never grants a vote - the same observable
 * effect a real unreachable/crashed peer would have on an election's vote tally.
 */
class RaftFailoverTest {

  @Test
  void committedEntrySurvivesLeaderFailoverInThreeNodeCluster() {
    RaftNode a = new RaftNode("A");
    RaftNode b = new RaftNode("B");
    RaftNode c = new RaftNode("C");

    // 1-2. A wins an election and becomes leader.
    ElectionCoordinator aElection =
        new ElectionCoordinator(
            a,
            List.of(
                new RaftPeer("B", new InProcessRaftPeerConnection(b)),
                new RaftPeer("C", new InProcessRaftPeerConnection(c))),
            3);
    assertTrue(aElection.startElection());
    assertEquals(RaftState.LEADER, a.getState());
    assertEquals(1, a.getCurrentTerm());

    // 3. A replicates and commits an entry to a majority (all three, here).
    a.getLog().appendCommand(a.getCurrentTerm(), "cmd-1");
    RaftLogReplicator aReplicator =
        new RaftLogReplicator(
            a,
            List.of(
                new AppendEntriesPeer("B", new InProcessAppendEntriesConnection(b)),
                new AppendEntriesPeer("C", new InProcessAppendEntriesConnection(c))),
            3);
    aReplicator.initializeForNewLeader();
    aReplicator.replicate(
        a.getCommitIndex()); // round 1: replicates the entry, A's commitIndex -> 1
    aReplicator.replicate(a.getCommitIndex()); // round 2: carries leaderCommit=1 to B and C

    assertEquals(1, a.getCommitIndex());
    assertEquals(1, b.getCommitIndex());
    assertEquals(1, c.getCommitIndex());
    assertEquals("cmd-1", b.getLog().get(1).orElseThrow().command());
    assertEquals("cmd-1", c.getLog().get(1).orElseThrow().command());

    // 4. A becomes unavailable - nothing further is ever invoked on `a` after this point.

    // 5-7. B's election timeout fires; B starts an election. A is modeled as unreachable (never
    // grants a vote); C is still reachable and votes for B, giving B a 2-of-3 majority.
    RaftPeerConnection unreachableA = request -> new RequestVoteResponse(request.term(), false);
    ElectionCoordinator bElection =
        new ElectionCoordinator(
            b,
            List.of(
                new RaftPeer("A", unreachableA),
                new RaftPeer("C", new InProcessRaftPeerConnection(c))),
            3);
    ElectionTimeout bTimeout = new ElectionTimeout(100, 100, new Random(1));
    RaftElectionDriver bDriver = new RaftElectionDriver(b, bElection, bTimeout, 0);

    bDriver.tick(150); // past the deadline - election starts automatically

    // 8. B is the new leader, in a newer term than A's.
    assertEquals(RaftState.LEADER, b.getState());
    assertEquals(2, b.getCurrentTerm());
    assertTrue(b.getCurrentTerm() > a.getCurrentTerm());

    // 9. The previously committed entry survived the leadership change.
    assertEquals("cmd-1", b.getLog().get(1).orElseThrow().command());
    assertEquals(1, b.getCommitIndex());

    // The new leader can keep the cluster moving: a fresh current-term entry still commits by
    // majority against the surviving nodes. The cluster is still configured as 3 voting members -
    // A is merely unreachable, not removed - so clusterSize stays 3 even though A is omitted from
    // this replicator's peer list.
    b.getLog().appendCommand(b.getCurrentTerm(), "cmd-2");
    RaftLogReplicator bReplicator =
        new RaftLogReplicator(
            b, List.of(new AppendEntriesPeer("C", new InProcessAppendEntriesConnection(c))), 3);
    bReplicator.initializeForNewLeader();
    bReplicator.replicate(b.getCommitIndex());

    // B (self) + C = 2 of the still-3-node cluster, which meets majority(3)=2 even with A
    // unreachable.
    assertEquals(2, b.getCommitIndex());
    assertEquals("cmd-2", c.getLog().get(2).orElseThrow().command());

    // A, having lost leadership, can never again advance its own commitIndex through replication:
    // it is simply never invoked again, and would fail its own "must be LEADER" guard the moment
    // it tried, since any future contact with the newer term would step it down to FOLLOWER first.
  }

  @Test
  void leaderStepsDownOnHigherTermDuringReplication() {
    RaftNode leader = new RaftNode("broker-1");
    leader.becomeCandidate();
    leader.becomeLeader();

    RaftNode aheadPeer = new RaftNode("broker-2");
    aheadPeer.advanceTerm(5);

    RaftLogReplicator replicator =
        new RaftLogReplicator(
            leader,
            List.of(
                new AppendEntriesPeer("broker-2", new InProcessAppendEntriesConnection(aheadPeer))),
            2);
    replicator.initializeForNewLeader();
    replicator.replicate(leader.getCommitIndex());

    assertEquals(RaftState.FOLLOWER, leader.getState());
    assertEquals(5, leader.getCurrentTerm());
  }

  @Test
  void candidateStepsDownOnHigherTermDuringElection() {
    RaftNode candidateNode = new RaftNode("broker-1");
    RaftNode aheadPeer = new RaftNode("broker-2");
    aheadPeer.advanceTerm(5);

    ElectionCoordinator election =
        new ElectionCoordinator(
            candidateNode,
            List.of(new RaftPeer("broker-2", new InProcessRaftPeerConnection(aheadPeer))),
            2);

    boolean elected = election.startElection();

    assertEquals(false, elected);
    assertEquals(RaftState.FOLLOWER, candidateNode.getState());
    assertEquals(5, candidateNode.getCurrentTerm());
  }
}
