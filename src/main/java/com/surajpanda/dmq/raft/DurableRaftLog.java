package com.surajpanda.dmq.raft;

import java.util.List;

/**
 * A RaftLog whose mutations are mirrored to a RaftLogStore so entries survive a restart. The
 * in-memory matching/conflict-resolution/duplicate-safety algorithm is entirely inherited from
 * RaftLog, unmodified - this class only adds a synchronous write-through to durable storage after
 * each successful mutation, and replays the store's contents back through RaftLog's own
 * appendEntries() on construction to reconstruct an identical in-memory log.
 *
 * <p>Recovery works by feeding every recovered entry through appendEntries(0, 0, recovered): since
 * the log starts empty, the sentinel prevLogIndex=0 always matches, and the entries are appended in
 * order exactly as appendEntries already knows how to do - no new access into RaftLog's internals
 * is needed for recovery.
 */
public class DurableRaftLog extends RaftLog {

  private final RaftLogStore store;

  public DurableRaftLog(RaftLogStore store) {
    this.store = store;

    List<LogEntry> recovered = store.loadAll();

    if (!recovered.isEmpty() && !super.appendEntries(0, 0, recovered)) {
      throw new RaftPersistenceException(
          "Failed to reconstruct RaftLog from durable store - recovered entries do not form a"
              + " valid log",
          null);
    }
  }

  @Override
  public synchronized LogEntry appendCommand(long term, String command) {
    LogEntry entry = super.appendCommand(term, command);
    store.append(entry);
    return entry;
  }

  @Override
  public synchronized boolean appendEntries(
      long prevLogIndex, long prevLogTerm, List<LogEntry> newEntries) {

    boolean success = super.appendEntries(prevLogIndex, prevLogTerm, newEntries);

    if (success && !newEntries.isEmpty()) {
      store.truncateFrom(prevLogIndex + 1);
      for (LogEntry entry : newEntries) {
        store.append(entry);
      }
    }

    return success;
  }
}
