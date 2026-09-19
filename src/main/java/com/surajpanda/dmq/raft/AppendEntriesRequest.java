package com.surajpanda.dmq.raft;

import java.util.List;

/**
 * leaderCommit is carried per the Raft AppendEntries protocol even though nothing in this milestone
 * advances a commit index or applies entries to a state machine - that arrives in Commit 6.
 */
public record AppendEntriesRequest(
    long term,
    String leaderId,
    long prevLogIndex,
    long prevLogTerm,
    List<LogEntry> entries,
    long leaderCommit) {

  public AppendEntriesRequest {
    entries = List.copyOf(entries);
  }
}
