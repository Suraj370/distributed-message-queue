package com.surajpanda.dmq.raft;

import java.util.List;

/**
 * Durable storage for Raft log entries, backing DurableRaftLog. Conceptually distinct from the
 * queue's WAL (Wal/Partition): this stores the replicated consensus log, not queue messages.
 */
public interface RaftLogStore {

  void append(LogEntry entry);

  /**
   * Discards every previously stored entry at or after index - used when AppendEntries'
   * conflict-resolution rule replaces a conflicting suffix.
   */
  void truncateFrom(long index);

  List<LogEntry> loadAll();
}
