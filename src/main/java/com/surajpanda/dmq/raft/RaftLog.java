package com.surajpanda.dmq.raft;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * In-memory, 1-indexed Raft log. Index 0 is the sentinel "before the log begins" position used by
 * prevLogIndex/prevLogTerm, matching the Raft paper's convention.
 *
 * <p>Not persisted in this milestone. appendEntries() applies the whole log-matching /
 * conflict-resolution rule as a single synchronized operation rather than exposing separate
 * match-check / truncate / append steps a caller could interleave - that keeps log consistency a
 * one-method unit instead of something guarded by locks scattered across several call sites. A
 * durable implementation can sit behind this same method shape later without callers changing.
 */
public class RaftLog {

  private final List<LogEntry> entries = new ArrayList<>();

  public synchronized long lastIndex() {
    return entries.size();
  }

  public synchronized long lastTerm() {
    return entries.isEmpty() ? 0 : entries.get(entries.size() - 1).term();
  }

  public synchronized Optional<LogEntry> get(long index) {
    if (index < 1 || index > entries.size()) {
      return Optional.empty();
    }
    return Optional.of(entries.get((int) index - 1));
  }

  public synchronized List<LogEntry> entriesFrom(long fromIndex) {
    if (fromIndex > entries.size()) {
      return List.of();
    }
    int start = (int) Math.max(fromIndex, 1) - 1;
    return List.copyOf(entries.subList(start, entries.size()));
  }

  /** Leader-side: proposes a new command at the next index, in the leader's current term. */
  public synchronized LogEntry appendCommand(long term, String command) {
    LogEntry entry = new LogEntry(term, entries.size() + 1L, command);
    entries.add(entry);
    return entry;
  }

  /**
   * Applies AppendEntries' log-matching and conflict-resolution rule atomically: rejects unless
   * prevLogIndex/prevLogTerm match an existing entry (or prevLogIndex is the 0 sentinel); then
   * walks the new entries, leaving an existing entry alone when its term already matches (so a
   * duplicate/retried AppendEntries is a no-op), and truncating the conflicting suffix before
   * appending when it doesn't.
   */
  public synchronized boolean appendEntries(
      long prevLogIndex, long prevLogTerm, List<LogEntry> newEntries) {

    if (!hasMatchingEntry(prevLogIndex, prevLogTerm)) {
      return false;
    }

    long index = prevLogIndex + 1;

    for (LogEntry newEntry : newEntries) {

      if (newEntry.index() != index) {
        throw new IllegalArgumentException(
            "Entry index out of sequence: expected=" + index + ", was=" + newEntry.index());
      }

      int position = (int) index - 1;

      if (position < entries.size()) {
        if (entries.get(position).term() == newEntry.term()) {
          index++;
          continue; // already present - duplicate/retried AppendEntries, nothing to do
        }
        entries.subList(position, entries.size()).clear(); // conflicting suffix, discard it
      }

      entries.add(newEntry);
      index++;
    }

    return true;
  }

  /**
   * Restores the suffix starting at the given 0-indexed list position to exactly savedSuffix,
   * discarding whatever currently occupies that position onward. Used by DurableRaftLog to roll
   * back an in-memory mutation whose matching durable write failed, so memory can never end up
   * ahead of what was actually persisted.
   */
  protected synchronized void restoreSuffix(int fromPosition, List<LogEntry> savedSuffix) {
    entries.subList(fromPosition, entries.size()).clear();
    entries.addAll(savedSuffix);
  }

  private boolean hasMatchingEntry(long prevLogIndex, long prevLogTerm) {

    if (prevLogIndex == 0) {
      return true;
    }

    if (prevLogIndex > entries.size()) {
      return false;
    }

    return entries.get((int) prevLogIndex - 1).term() == prevLogTerm;
  }
}
