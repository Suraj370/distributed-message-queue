package com.surajpanda.dmq.raft;

/**
 * A leadership/liveness signal only. No log entries yet - those are deferred to the log-replication
 * stage, where this message will grow into a full AppendEntries RPC.
 */
public record Heartbeat(long term, String leaderId) {}
