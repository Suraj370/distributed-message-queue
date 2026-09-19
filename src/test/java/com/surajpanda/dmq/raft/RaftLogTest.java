package com.surajpanda.dmq.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class RaftLogTest {

  @Test
  void emptyLogHasNoLastEntry() {
    RaftLog log = new RaftLog();
    assertEquals(0, log.lastIndex());
    assertEquals(0, log.lastTerm());
    assertTrue(log.get(1).isEmpty());
  }

  @Test
  void emptyFollowerAcceptsFirstEntry() {
    RaftLog log = new RaftLog();

    boolean accepted = log.appendEntries(0, 0, List.of(new LogEntry(1, 1, "cmd-1")));

    assertTrue(accepted);
    assertEquals(1, log.lastIndex());
    assertEquals(1, log.lastTerm());
    assertEquals("cmd-1", log.get(1).orElseThrow().command());
  }

  @Test
  void acceptsMatchingPrevLogIndexAndTerm() {
    RaftLog log = new RaftLog();
    log.appendEntries(0, 0, List.of(new LogEntry(1, 1, "cmd-1")));

    boolean accepted = log.appendEntries(1, 1, List.of(new LogEntry(1, 2, "cmd-2")));

    assertTrue(accepted);
    assertEquals(2, log.lastIndex());
    assertEquals("cmd-2", log.get(2).orElseThrow().command());
  }

  @Test
  void rejectsMismatchingPrevLogTerm() {
    RaftLog log = new RaftLog();
    log.appendEntries(0, 0, List.of(new LogEntry(1, 1, "cmd-1")));

    boolean accepted = log.appendEntries(1, 99, List.of(new LogEntry(2, 2, "cmd-2")));

    assertFalse(accepted);
    assertEquals(1, log.lastIndex()); // untouched
  }

  @Test
  void rejectsMissingPrevLogIndex() {
    RaftLog log = new RaftLog();

    boolean accepted = log.appendEntries(5, 1, List.of(new LogEntry(1, 6, "cmd-6")));

    assertFalse(accepted);
    assertEquals(0, log.lastIndex());
  }

  @Test
  void appendsMultipleEntriesInOrder() {
    RaftLog log = new RaftLog();

    boolean accepted =
        log.appendEntries(
            0,
            0,
            List.of(
                new LogEntry(1, 1, "cmd-1"),
                new LogEntry(1, 2, "cmd-2"),
                new LogEntry(1, 3, "cmd-3")));

    assertTrue(accepted);
    assertEquals(3, log.lastIndex());
    assertEquals("cmd-1", log.get(1).orElseThrow().command());
    assertEquals("cmd-2", log.get(2).orElseThrow().command());
    assertEquals("cmd-3", log.get(3).orElseThrow().command());
  }

  @Test
  void duplicateAppendEntriesDoesNotDuplicateEntries() {
    RaftLog log = new RaftLog();
    log.appendEntries(0, 0, List.of(new LogEntry(1, 1, "cmd-1"), new LogEntry(1, 2, "cmd-2")));

    boolean accepted =
        log.appendEntries(0, 0, List.of(new LogEntry(1, 1, "cmd-1"), new LogEntry(1, 2, "cmd-2")));

    assertTrue(accepted);
    assertEquals(2, log.lastIndex());
    assertEquals("cmd-2", log.get(2).orElseThrow().command());
  }

  @Test
  void conflictingFollowerSuffixIsReplacedByLeaderEntries() {
    RaftLog log = new RaftLog();
    log.appendEntries(
        0,
        0,
        List.of(
            new LogEntry(1, 1, "cmd-1"),
            new LogEntry(1, 2, "stale-2"),
            new LogEntry(1, 3, "stale-3")));

    // A new leader (term 2) overwrites index 2 onward with different entries.
    boolean accepted = log.appendEntries(1, 1, List.of(new LogEntry(2, 2, "cmd-2v2")));

    assertTrue(accepted);
    assertEquals(2, log.lastIndex());
    assertEquals("cmd-1", log.get(1).orElseThrow().command());
    assertEquals("cmd-2v2", log.get(2).orElseThrow().command());
    assertEquals(2, log.get(2).orElseThrow().term());
  }

  @Test
  void entriesFromReturnsSuffixInOrder() {
    RaftLog log = new RaftLog();
    log.appendEntries(
        0,
        0,
        List.of(
            new LogEntry(1, 1, "cmd-1"), new LogEntry(1, 2, "cmd-2"), new LogEntry(1, 3, "cmd-3")));

    List<LogEntry> suffix = log.entriesFrom(2);

    assertEquals(2, suffix.size());
    assertEquals("cmd-2", suffix.get(0).command());
    assertEquals("cmd-3", suffix.get(1).command());
  }

  @Test
  void entriesFromPastEndIsEmpty() {
    RaftLog log = new RaftLog();
    log.appendEntries(0, 0, List.of(new LogEntry(1, 1, "cmd-1")));

    assertTrue(log.entriesFrom(5).isEmpty());
  }

  @Test
  void appendCommandAssignsSequentialIndices() {
    RaftLog log = new RaftLog();

    LogEntry first = log.appendCommand(1, "cmd-1");
    LogEntry second = log.appendCommand(1, "cmd-2");

    assertEquals(1, first.index());
    assertEquals(2, second.index());
    assertEquals(2, log.lastIndex());
  }

  @Test
  void twoLogInstancesAreIndependent() {
    RaftLog logA = new RaftLog();
    RaftLog logB = new RaftLog();

    logA.appendEntries(0, 0, List.of(new LogEntry(1, 1, "cmd-a")));

    assertEquals(1, logA.lastIndex());
    assertEquals(0, logB.lastIndex());
  }
}
