package com.surajpanda.dmq.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class RequestVoteTest {

  @Test
  void shouldHaveNoVotedForInitially() {
    RaftNode node = new RaftNode("broker-1");
    assertNull(node.getVotedFor());
  }

  @Test
  void shouldGrantVoteForCurrentTermRequest() {
    RaftNode node = new RaftNode("broker-1");

    RequestVoteResponse response =
        node.handleRequestVote(new RequestVoteRequest(0, "broker-2", 0, 0));

    assertTrue(response.voteGranted());
    assertEquals(0, response.term());
    assertEquals("broker-2", node.getVotedFor());
  }

  @Test
  void shouldGrantVoteForNewerTermRequest() {
    RaftNode node = new RaftNode("broker-1");

    RequestVoteResponse response =
        node.handleRequestVote(new RequestVoteRequest(5, "broker-2", 0, 0));

    assertTrue(response.voteGranted());
    assertEquals(5, response.term());
    assertEquals("broker-2", node.getVotedFor());
  }

  @Test
  void shouldRegrantVoteToSameCandidateRequestingAgainInSameTerm() {
    RaftNode node = new RaftNode("broker-1");
    node.handleRequestVote(new RequestVoteRequest(3, "broker-2", 0, 0));

    RequestVoteResponse response =
        node.handleRequestVote(new RequestVoteRequest(3, "broker-2", 0, 0));

    assertTrue(response.voteGranted());
    assertEquals("broker-2", node.getVotedFor());
  }

  @Test
  void shouldRejectSecondCandidateInSameTerm() {
    RaftNode node = new RaftNode("broker-1");
    node.handleRequestVote(new RequestVoteRequest(3, "broker-2", 0, 0));

    RequestVoteResponse response =
        node.handleRequestVote(new RequestVoteRequest(3, "broker-3", 0, 0));

    assertFalse(response.voteGranted());
    assertEquals("broker-2", node.getVotedFor());
  }

  @Test
  void shouldRejectStaleTermRequest() {
    RaftNode node = new RaftNode("broker-1");
    node.advanceTerm(5);

    RequestVoteResponse response =
        node.handleRequestVote(new RequestVoteRequest(3, "broker-2", 0, 0));

    assertFalse(response.voteGranted());
    assertEquals(5, response.term());
    assertNull(node.getVotedFor());
  }

  @Test
  void shouldUpdateCurrentTermOnNewerTermRequest() {
    RaftNode node = new RaftNode("broker-1");

    node.handleRequestVote(new RequestVoteRequest(7, "broker-2", 0, 0));

    assertEquals(7, node.getCurrentTerm());
  }

  @Test
  void shouldBecomeFollowerOnNewerTermRequest() {
    RaftNode node = new RaftNode("broker-1");
    node.becomeCandidate();
    node.becomeLeader();

    node.handleRequestVote(new RequestVoteRequest(9, "broker-2", 0, 0));

    assertEquals(RaftState.FOLLOWER, node.getState());
  }

  @Test
  void shouldReturnCurrentTermInResponse() {
    RaftNode node = new RaftNode("broker-1");
    node.advanceTerm(4);

    RequestVoteResponse response =
        node.handleRequestVote(new RequestVoteRequest(4, "broker-2", 0, 0));

    assertEquals(4, response.term());
  }

  @Test
  void shouldKeepVoteStateIndependentBetweenNodes() {
    RaftNode nodeA = new RaftNode("broker-1");
    RaftNode nodeB = new RaftNode("broker-2");

    nodeA.handleRequestVote(new RequestVoteRequest(1, "broker-3", 0, 0));

    assertEquals("broker-3", nodeA.getVotedFor());
    assertNull(nodeB.getVotedFor());
  }

  @Test
  void shouldVoteForItselfWhenBecomingCandidate() {
    RaftNode node = new RaftNode("broker-1");

    node.becomeCandidate();

    assertEquals("broker-1", node.getVotedFor());
  }

  @Test
  void shouldRejectSameTermVoteRequestFromAnotherCandidateWhileCandidate() {
    RaftNode node = new RaftNode("broker-1");
    node.becomeCandidate();

    RequestVoteResponse response =
        node.handleRequestVote(new RequestVoteRequest(node.getCurrentTerm(), "broker-2", 0, 0));

    assertFalse(response.voteGranted());
    assertEquals("broker-1", node.getVotedFor());
  }

  @Test
  void shouldHandleNewerTermVoteRequestCorrectlyWhileCandidate() {
    RaftNode node = new RaftNode("broker-1");
    node.becomeCandidate();
    long newerTerm = node.getCurrentTerm() + 1;

    RequestVoteResponse response =
        node.handleRequestVote(new RequestVoteRequest(newerTerm, "broker-2", 0, 0));

    assertTrue(response.voteGranted());
    assertEquals(newerTerm, response.term());
    assertEquals(RaftState.FOLLOWER, node.getState());
    assertEquals("broker-2", node.getVotedFor());
  }

  @Test
  void shouldNotGrantSameTermVoteToAnotherCandidateAfterBecomingLeader() {
    RaftNode node = new RaftNode("broker-1");
    node.becomeCandidate();
    node.becomeLeader();

    RequestVoteResponse response =
        node.handleRequestVote(new RequestVoteRequest(node.getCurrentTerm(), "broker-2", 0, 0));

    assertFalse(response.voteGranted());
    assertEquals("broker-1", node.getVotedFor());
  }

  @Test
  void shouldStillGrantValidFollowerVoteRequest() {
    RaftNode node = new RaftNode("broker-1");

    RequestVoteResponse response =
        node.handleRequestVote(new RequestVoteRequest(2, "broker-2", 0, 0));

    assertTrue(response.voteGranted());
    assertEquals("broker-2", node.getVotedFor());
    assertEquals(RaftState.FOLLOWER, node.getState());
  }

  // --- Log up-to-date checks (Raft §5.4.1) ---

  @Test
  void candidateWithHigherLastLogTermIsEligible() {
    RaftNode voter = new RaftNode("broker-1");
    voter.getLog().appendCommand(1, "cmd-1"); // voter's lastLogTerm=1, lastLogIndex=1

    RequestVoteResponse response =
        voter.handleRequestVote(new RequestVoteRequest(2, "broker-2", 1, 2));

    assertTrue(response.voteGranted());
  }

  @Test
  void candidateWithSameLastLogTermAndHigherLastLogIndexIsEligible() {
    RaftNode voter = new RaftNode("broker-1");
    voter.getLog().appendCommand(1, "cmd-1"); // voter: lastLogTerm=1, lastLogIndex=1

    RequestVoteResponse response =
        voter.handleRequestVote(new RequestVoteRequest(1, "broker-2", 2, 1));

    assertTrue(response.voteGranted());
  }

  @Test
  void candidateWithSameTermButShorterLogIsRejected() {
    RaftNode voter = new RaftNode("broker-1");
    voter.getLog().appendCommand(1, "cmd-1");
    voter.getLog().appendCommand(1, "cmd-2"); // voter: lastLogTerm=1, lastLogIndex=2

    RequestVoteResponse response =
        voter.handleRequestVote(new RequestVoteRequest(1, "broker-2", 1, 1));

    assertFalse(response.voteGranted());
    assertNull(voter.getVotedFor());
  }

  @Test
  void candidateWithLowerLastLogTermIsRejectedEvenIfItsIndexIsHigher() {
    RaftNode voter = new RaftNode("broker-1");
    voter.getLog().appendCommand(2, "cmd-1"); // voter: lastLogTerm=2, lastLogIndex=1

    // Candidate has a longer log (index 5) but it's from an older term (1) - not up-to-date.
    RequestVoteResponse response =
        voter.handleRequestVote(new RequestVoteRequest(2, "broker-2", 5, 1));

    assertFalse(response.voteGranted());
    assertNull(voter.getVotedFor());
  }

  @Test
  void emptyVoterLogAcceptsAnEmptyCandidateLog() {
    RaftNode voter = new RaftNode("broker-1");

    RequestVoteResponse response =
        voter.handleRequestVote(new RequestVoteRequest(1, "broker-2", 0, 0));

    assertTrue(response.voteGranted());
  }

  @Test
  void emptyVoterLogAcceptsACandidateWithEntries() {
    RaftNode voter = new RaftNode("broker-1");

    RequestVoteResponse response =
        voter.handleRequestVote(new RequestVoteRequest(1, "broker-2", 3, 1));

    assertTrue(response.voteGranted());
  }

  @Test
  void voterWithMoreUpToDateLogRejectsAStaleCandidate() {
    RaftNode voter = new RaftNode("broker-1");
    voter.getLog().appendCommand(1, "cmd-1");
    voter.getLog().appendCommand(2, "cmd-2"); // voter: lastLogTerm=2, lastLogIndex=2

    RequestVoteResponse response =
        voter.handleRequestVote(new RequestVoteRequest(2, "broker-2", 0, 0));

    assertFalse(response.voteGranted());
    assertNull(voter.getVotedFor());
  }

  @Test
  void logCheckDoesNotPreventGrantingToTheSameCandidateAgainInTheSameTerm() {
    RaftNode voter = new RaftNode("broker-1");
    voter.getLog().appendCommand(1, "cmd-1");

    RequestVoteRequest request = new RequestVoteRequest(2, "broker-2", 1, 1);
    voter.handleRequestVote(request);
    RequestVoteResponse response = voter.handleRequestVote(request);

    assertTrue(response.voteGranted());
    assertEquals("broker-2", voter.getVotedFor());
  }

  @Test
  void logCheckDoesNotPreventNewerTermFromUpdatingTermAndBecomingFollower() {
    RaftNode voter = new RaftNode("broker-1");
    voter.getLog().appendCommand(5, "cmd-1"); // voter far ahead in log term
    voter.becomeCandidate();
    voter.becomeLeader();

    // Candidate's log is behind (term 0), so the vote itself must still be rejected...
    RequestVoteResponse response =
        voter.handleRequestVote(new RequestVoteRequest(9, "broker-2", 0, 0));

    // ...but the newer term (9) must still be adopted and the node must still step down.
    assertFalse(response.voteGranted());
    assertEquals(9, voter.getCurrentTerm());
    assertEquals(RaftState.FOLLOWER, voter.getState());
  }
}
