package com.surajpanda.dmq.raft;

/**
 * lastLogIndex/lastLogTerm describe the candidate's log as of the moment it started this election,
 * so a voter can apply Raft's log up-to-date check (§5.4.1): a candidate whose log is not at least
 * as up-to-date as the voter's own log must never receive that voter's vote, even if it is
 * otherwise eligible on term/votedFor grounds alone.
 */
public record RequestVoteRequest(
    long term, String candidateId, long lastLogIndex, long lastLogTerm) {}
