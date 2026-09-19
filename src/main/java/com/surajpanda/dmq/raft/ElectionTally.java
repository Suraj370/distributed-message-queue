package com.surajpanda.dmq.raft;

import java.util.HashSet;
import java.util.Set;

/**
 * Explicit, in-memory vote accounting for a single election term. A response only counts toward the
 * majority when it is granted, comes from a configured eligible voter, and matches the term this
 * election was started in; a term mismatch (older or newer) is left for the caller to handle
 * separately (e.g. stepping down on a newer term), never silently folded into the tally.
 */
public class ElectionTally {

  private final long electionTerm;
  private final Set<String> eligibleVoters;
  private final Set<String> grantedVotes = new HashSet<>();

  public ElectionTally(long electionTerm, String candidateId, Set<String> eligibleVoters) {
    this.electionTerm = electionTerm;
    this.eligibleVoters = Set.copyOf(eligibleVoters);
    this.grantedVotes.add(candidateId);
  }

  public void recordResponse(String voterId, RequestVoteResponse response) {

    if (response.term() != electionTerm) {
      return;
    }

    if (!response.voteGranted()) {
      return;
    }

    if (!eligibleVoters.contains(voterId)) {
      return;
    }

    grantedVotes.add(voterId);
  }

  public int voteCount() {
    return grantedVotes.size();
  }

  public boolean hasMajority(int clusterSize) {
    int majority = (clusterSize / 2) + 1;
    return grantedVotes.size() >= majority;
  }
}
