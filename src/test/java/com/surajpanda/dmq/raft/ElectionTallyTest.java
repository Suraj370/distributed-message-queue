package com.surajpanda.dmq.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import org.junit.jupiter.api.Test;

class ElectionTallyTest {

  @Test
  void shouldCountSelfVoteOnConstruction() {
    ElectionTally tally = new ElectionTally(1, "broker-1", Set.of("broker-2", "broker-3"));
    assertEquals(1, tally.voteCount());
  }

  @Test
  void shouldCountGrantedVoteFromEligiblePeer() {
    ElectionTally tally = new ElectionTally(1, "broker-1", Set.of("broker-2"));

    tally.recordResponse("broker-2", new RequestVoteResponse(1, true));

    assertEquals(2, tally.voteCount());
  }

  @Test
  void shouldCountDuplicateResponseFromSamePeerOnlyOnce() {
    ElectionTally tally = new ElectionTally(1, "broker-1", Set.of("broker-2"));

    tally.recordResponse("broker-2", new RequestVoteResponse(1, true));
    tally.recordResponse("broker-2", new RequestVoteResponse(1, true));

    assertEquals(2, tally.voteCount());
  }

  @Test
  void shouldNotCountRejectedVote() {
    ElectionTally tally = new ElectionTally(1, "broker-1", Set.of("broker-2"));

    tally.recordResponse("broker-2", new RequestVoteResponse(1, false));

    assertEquals(1, tally.voteCount());
  }

  @Test
  void shouldNotCountVoteFromUnknownNode() {
    ElectionTally tally = new ElectionTally(1, "broker-1", Set.of("broker-2"));

    tally.recordResponse("broker-99", new RequestVoteResponse(1, true));

    assertEquals(1, tally.voteCount());
  }

  @Test
  void shouldNotCountResponseFromAnOlderTerm() {
    ElectionTally tally = new ElectionTally(2, "broker-1", Set.of("broker-2"));

    tally.recordResponse("broker-2", new RequestVoteResponse(1, true));

    assertEquals(1, tally.voteCount());
  }

  @Test
  void shouldNotCountResponseFromANewerTerm() {
    ElectionTally tally = new ElectionTally(2, "broker-1", Set.of("broker-2"));

    tally.recordResponse("broker-2", new RequestVoteResponse(3, true));

    assertEquals(1, tally.voteCount());
  }

  @Test
  void shouldRequireOneVoteMajorityInSingleNodeCluster() {
    ElectionTally tally = new ElectionTally(1, "broker-1", Set.of());
    assertTrue(tally.hasMajority(1));
  }

  @Test
  void shouldRequireTwoVoteMajorityInThreeNodeCluster() {
    ElectionTally tally = new ElectionTally(1, "broker-1", Set.of("broker-2", "broker-3"));
    assertFalse(tally.hasMajority(3));

    tally.recordResponse("broker-2", new RequestVoteResponse(1, true));
    assertTrue(tally.hasMajority(3));
  }

  @Test
  void shouldRequireThreeVoteMajorityInFiveNodeCluster() {
    ElectionTally tally =
        new ElectionTally(1, "broker-1", Set.of("broker-2", "broker-3", "broker-4", "broker-5"));

    tally.recordResponse("broker-2", new RequestVoteResponse(1, true));
    assertFalse(tally.hasMajority(5));

    tally.recordResponse("broker-3", new RequestVoteResponse(1, true));
    assertTrue(tally.hasMajority(5));
  }
}
