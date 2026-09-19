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

    RequestVoteResponse response = node.handleRequestVote(new RequestVoteRequest(0, "broker-2"));

    assertTrue(response.voteGranted());
    assertEquals(0, response.term());
    assertEquals("broker-2", node.getVotedFor());
  }

  @Test
  void shouldGrantVoteForNewerTermRequest() {
    RaftNode node = new RaftNode("broker-1");

    RequestVoteResponse response = node.handleRequestVote(new RequestVoteRequest(5, "broker-2"));

    assertTrue(response.voteGranted());
    assertEquals(5, response.term());
    assertEquals("broker-2", node.getVotedFor());
  }

  @Test
  void shouldRegrantVoteToSameCandidateRequestingAgainInSameTerm() {
    RaftNode node = new RaftNode("broker-1");
    node.handleRequestVote(new RequestVoteRequest(3, "broker-2"));

    RequestVoteResponse response = node.handleRequestVote(new RequestVoteRequest(3, "broker-2"));

    assertTrue(response.voteGranted());
    assertEquals("broker-2", node.getVotedFor());
  }

  @Test
  void shouldRejectSecondCandidateInSameTerm() {
    RaftNode node = new RaftNode("broker-1");
    node.handleRequestVote(new RequestVoteRequest(3, "broker-2"));

    RequestVoteResponse response = node.handleRequestVote(new RequestVoteRequest(3, "broker-3"));

    assertFalse(response.voteGranted());
    assertEquals("broker-2", node.getVotedFor());
  }

  @Test
  void shouldRejectStaleTermRequest() {
    RaftNode node = new RaftNode("broker-1");
    node.advanceTerm(5);

    RequestVoteResponse response = node.handleRequestVote(new RequestVoteRequest(3, "broker-2"));

    assertFalse(response.voteGranted());
    assertEquals(5, response.term());
    assertNull(node.getVotedFor());
  }

  @Test
  void shouldUpdateCurrentTermOnNewerTermRequest() {
    RaftNode node = new RaftNode("broker-1");

    node.handleRequestVote(new RequestVoteRequest(7, "broker-2"));

    assertEquals(7, node.getCurrentTerm());
  }

  @Test
  void shouldBecomeFollowerOnNewerTermRequest() {
    RaftNode node = new RaftNode("broker-1");
    node.becomeCandidate();
    node.becomeLeader();

    node.handleRequestVote(new RequestVoteRequest(9, "broker-2"));

    assertEquals(RaftState.FOLLOWER, node.getState());
  }

  @Test
  void shouldReturnCurrentTermInResponse() {
    RaftNode node = new RaftNode("broker-1");
    node.advanceTerm(4);

    RequestVoteResponse response = node.handleRequestVote(new RequestVoteRequest(4, "broker-2"));

    assertEquals(4, response.term());
  }

  @Test
  void shouldKeepVoteStateIndependentBetweenNodes() {
    RaftNode nodeA = new RaftNode("broker-1");
    RaftNode nodeB = new RaftNode("broker-2");

    nodeA.handleRequestVote(new RequestVoteRequest(1, "broker-3"));

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
        node.handleRequestVote(new RequestVoteRequest(node.getCurrentTerm(), "broker-2"));

    assertFalse(response.voteGranted());
    assertEquals("broker-1", node.getVotedFor());
  }

  @Test
  void shouldHandleNewerTermVoteRequestCorrectlyWhileCandidate() {
    RaftNode node = new RaftNode("broker-1");
    node.becomeCandidate();
    long newerTerm = node.getCurrentTerm() + 1;

    RequestVoteResponse response =
        node.handleRequestVote(new RequestVoteRequest(newerTerm, "broker-2"));

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
        node.handleRequestVote(new RequestVoteRequest(node.getCurrentTerm(), "broker-2"));

    assertFalse(response.voteGranted());
    assertEquals("broker-1", node.getVotedFor());
  }

  @Test
  void shouldStillGrantValidFollowerVoteRequest() {
    RaftNode node = new RaftNode("broker-1");

    RequestVoteResponse response = node.handleRequestVote(new RequestVoteRequest(2, "broker-2"));

    assertTrue(response.voteGranted());
    assertEquals("broker-2", node.getVotedFor());
    assertEquals(RaftState.FOLLOWER, node.getState());
  }
}
