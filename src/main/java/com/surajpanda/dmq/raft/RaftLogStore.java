package com.surajpanda.dmq.raft;

import java.util.List;

/**
 * Durable storage for Raft log entries, backing DurableRaftLog. Conceptually distinct from the
 * queue's WAL (Wal/Partition): this stores the replicated consensus log, not queue messages.
 *
 * <p>replace() persists the complete intended logical log as a single durable operation - not an
 * incremental append/truncate record. That is a deliberate design choice: an incremental,
 * multi-write-call mutation cannot be made atomic against an observable persistence failure partway
 * through, because FileChannel.write() and force() can each fail independently after the other has
 * already durably landed some bytes. Persisting one complete snapshot per mutation, via a
 * temporary-file-plus-atomic-rename strategy, is what actually guarantees "old durable state OR
 * complete new durable state, never a partial mutation" - see FileRaftLogStore for the concrete
 * mechanism.
 */
public interface RaftLogStore {

  /**
   * Atomically replaces the durable log with fullLog, the complete intended logical state (already
   * in index order) - either the store durably ends up holding exactly fullLog, or a failure leaves
   * it holding exactly what it held before this call. Never a state part way between the two.
   */
  void replace(List<LogEntry> fullLog);

  List<LogEntry> loadAll();
}
