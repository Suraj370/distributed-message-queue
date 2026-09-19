package com.surajpanda.dmq.raft;

/**
 * Candidate log fields (lastLogIndex/lastLogTerm) are intentionally omitted. There is no replicated
 * log yet, so up-to-date checks are deferred to the log-replication stage.
 */
public record RequestVoteRequest(long term, String candidateId) {}
