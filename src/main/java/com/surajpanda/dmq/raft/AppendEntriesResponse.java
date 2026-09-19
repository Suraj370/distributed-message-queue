package com.surajpanda.dmq.raft;

/**
 * termAccepted and success are deliberately separate signals: termAccepted means the request came
 * from a leader whose term was current or newer (i.e. the leader is legitimate and alive), while
 * success means the prevLogIndex/prevLogTerm log-matching check also passed. A current-term leader
 * can be alive and legitimate even when a particular AppendEntries is rejected for a log mismatch -
 * callers that care about leader liveness (e.g. resetting an election timeout) should look at
 * termAccepted, not success.
 *
 * <p>matchIndex reports the responder's log length after processing this request (success or not),
 * so a future leader has a data point to build the nextIndex/matchIndex tracking introduced in
 * Commit 6. Nothing in this commit acts on it yet.
 */
public record AppendEntriesResponse(
    long term, boolean termAccepted, boolean success, long matchIndex) {}
