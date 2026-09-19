package com.surajpanda.dmq.raft;

public record AppendEntriesPeer(String nodeId, AppendEntriesConnection connection) {}
