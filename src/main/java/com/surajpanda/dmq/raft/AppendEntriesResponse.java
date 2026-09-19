package com.surajpanda.dmq.raft;

/**
 * matchIndex reports the responder's log length after processing this request (success or not), so
 * a future leader has a data point to build the nextIndex/matchIndex tracking introduced in Commit
 * 6. Nothing in this commit acts on it yet.
 */
public record AppendEntriesResponse(long term, boolean success, long matchIndex) {}
