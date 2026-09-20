package com.surajpanda.dmq.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.surajpanda.dmq.raft.support.FailingRaftLogStore;
import com.surajpanda.dmq.raft.support.PartiallyFailingRaftLogStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DurableRaftLogTest {

  @TempDir Path tempDir;

  @Test
  void emptyLogRecoversAsEmpty() {
    DurableRaftLog log = new DurableRaftLog(new FileRaftLogStore(tempDir.resolve("raft.log")));

    assertEquals(0, log.lastIndex());
    assertEquals(0, log.lastTerm());
  }

  @Test
  void appendedEntrySurvivesRestart() {
    Path file = tempDir.resolve("raft.log");

    DurableRaftLog before = new DurableRaftLog(new FileRaftLogStore(file));
    before.appendCommand(1, "cmd-1");

    DurableRaftLog after = new DurableRaftLog(new FileRaftLogStore(file));

    assertEquals(1, after.lastIndex());
    assertEquals("cmd-1", after.get(1).orElseThrow().command());
    assertEquals(1, after.get(1).orElseThrow().term());
  }

  @Test
  void multipleEntriesSurviveRestartInOrder() {
    Path file = tempDir.resolve("raft.log");

    DurableRaftLog before = new DurableRaftLog(new FileRaftLogStore(file));
    before.appendCommand(1, "cmd-1");
    before.appendCommand(1, "cmd-2");
    before.appendCommand(2, "cmd-3");

    DurableRaftLog after = new DurableRaftLog(new FileRaftLogStore(file));

    assertEquals(3, after.lastIndex());
    assertEquals("cmd-1", after.get(1).orElseThrow().command());
    assertEquals("cmd-2", after.get(2).orElseThrow().command());
    assertEquals("cmd-3", after.get(3).orElseThrow().command());
    assertEquals(2, after.get(3).orElseThrow().term());
  }

  @Test
  void indexesAndTermsRemainStableAcrossRestart() {
    Path file = tempDir.resolve("raft.log");

    DurableRaftLog before = new DurableRaftLog(new FileRaftLogStore(file));
    LogEntry original = before.appendCommand(5, "cmd-1");

    DurableRaftLog after = new DurableRaftLog(new FileRaftLogStore(file));
    LogEntry recovered = after.get(1).orElseThrow();

    assertEquals(original.index(), recovered.index());
    assertEquals(original.term(), recovered.term());
    assertEquals(original.command(), recovered.command());
  }

  @Test
  void conflictingSuffixReplacementSurvivesRestart() {
    Path file = tempDir.resolve("raft.log");

    DurableRaftLog before = new DurableRaftLog(new FileRaftLogStore(file));
    before.appendEntries(0, 0, List.of(new LogEntry(1, 1, "cmd-1"), new LogEntry(1, 2, "stale-2")));

    // A new leader (term 2) overwrites index 2 with a different entry.
    boolean accepted = before.appendEntries(1, 1, List.of(new LogEntry(2, 2, "cmd-2v2")));
    assertTrue(accepted);

    DurableRaftLog after = new DurableRaftLog(new FileRaftLogStore(file));

    assertEquals(2, after.lastIndex());
    assertEquals("cmd-1", after.get(1).orElseThrow().command());
    assertEquals("cmd-2v2", after.get(2).orElseThrow().command());
    assertEquals(2, after.get(2).orElseThrow().term());
  }

  @Test
  void recoveredLogRejectsMismatchingPrevLogTermJustLikeALiveLog() {
    Path file = tempDir.resolve("raft.log");

    DurableRaftLog before = new DurableRaftLog(new FileRaftLogStore(file));
    before.appendCommand(1, "cmd-1");

    DurableRaftLog after = new DurableRaftLog(new FileRaftLogStore(file));

    boolean accepted = after.appendEntries(1, 99, List.of(new LogEntry(2, 2, "cmd-2")));

    assertEquals(false, accepted);
    assertEquals(1, after.lastIndex()); // unchanged
  }

  @Test
  void aStrayIncompleteTempFileFromAnInterruptedWriteDoesNotAffectRecovery() throws Exception {
    // Every write to the real file is now a complete, atomically-promoted snapshot (see
    // FileRaftLogStore), so a leftover ".tmp" file from a process that crashed mid-write (before
    // the rename ever happened) is simply never read - loadAll() only ever looks at the real file.
    Path file = tempDir.resolve("raft.log");

    DurableRaftLog before = new DurableRaftLog(new FileRaftLogStore(file));
    before.appendCommand(1, "cmd-1");
    before.appendCommand(1, "cmd-2");

    Files.writeString(tempDir.resolve("raft.log.tmp"), "E|3|1|not-even-valid-base64!!!");

    DurableRaftLog after = new DurableRaftLog(new FileRaftLogStore(file));

    assertEquals(2, after.lastIndex());
    assertEquals("cmd-1", after.get(1).orElseThrow().command());
    assertEquals("cmd-2", after.get(2).orElseThrow().command());
  }

  @Test
  void corruptedRecordInTheRealFileIsAlwaysAHardFailure() throws Exception {
    // Unlike an append-only/tombstone format, every write to the real file is now a complete
    // snapshot written via temp-file-then-atomic-rename - there is no normal-operation scenario
    // where the real file ends up with a trailing partial record, so any corruption found in it
    // (start, middle, or end) is treated as a hard failure rather than leniently trimmed.
    Path file = tempDir.resolve("raft.log");

    DurableRaftLog before = new DurableRaftLog(new FileRaftLogStore(file));
    before.appendCommand(1, "cmd-1");

    Files.writeString(
        file,
        "GARBAGE-NOT-A-VALID-RECORD" + System.lineSeparator(),
        java.nio.file.StandardOpenOption.APPEND);

    assertThrows(
        RaftPersistenceException.class, () -> new DurableRaftLog(new FileRaftLogStore(file)));
  }

  @Test
  void appendPersistenceFailureDoesNotAdvanceTheInMemoryLog() {
    Path file = tempDir.resolve("raft.log");
    FailingRaftLogStore store = new FailingRaftLogStore(new FileRaftLogStore(file));
    DurableRaftLog log = new DurableRaftLog(store);

    log.appendCommand(1, "cmd-1"); // succeeds, durable

    store.failNextReplace();
    assertThrows(RaftPersistenceException.class, () -> log.appendCommand(1, "cmd-2"));

    // The failed append must not have left in-memory state ahead of what was actually persisted.
    assertEquals(1, log.lastIndex());
    assertEquals("cmd-1", log.get(1).orElseThrow().command());

    DurableRaftLog recovered = new DurableRaftLog(new FileRaftLogStore(file));
    assertEquals(1, recovered.lastIndex());
  }

  @Test
  void conflictingSuffixPersistenceFailureDoesNotAdvanceTheInMemoryLog() {
    Path file = tempDir.resolve("raft.log");
    FailingRaftLogStore store = new FailingRaftLogStore(new FileRaftLogStore(file));
    DurableRaftLog log = new DurableRaftLog(store);

    log.appendEntries(0, 0, List.of(new LogEntry(1, 1, "cmd-1"), new LogEntry(1, 2, "stale-2")));

    store.failNextReplace();
    assertThrows(
        RaftPersistenceException.class,
        () -> log.appendEntries(1, 1, List.of(new LogEntry(2, 2, "cmd-2v2"))));

    // The failed conflicting-suffix replacement must leave memory exactly as it was durably.
    assertEquals(2, log.lastIndex());
    assertEquals("stale-2", log.get(2).orElseThrow().command());
    assertEquals(1, log.get(2).orElseThrow().term());

    DurableRaftLog recovered = new DurableRaftLog(new FileRaftLogStore(file));
    assertEquals(2, recovered.lastIndex());
    assertEquals("stale-2", recovered.get(2).orElseThrow().command());
  }

  @Test
  void successfulMultiEntryAppendPersistsAllEntriesAsOneWholeLogReplacement() {
    Path file = tempDir.resolve("raft.log");
    DurableRaftLog log = new DurableRaftLog(new FileRaftLogStore(file));

    boolean accepted =
        log.appendEntries(
            0,
            0,
            List.of(
                new LogEntry(1, 1, "cmd-1"),
                new LogEntry(1, 2, "cmd-2"),
                new LogEntry(1, 3, "cmd-3")));

    assertTrue(accepted);

    DurableRaftLog recovered = new DurableRaftLog(new FileRaftLogStore(file));
    assertEquals(3, recovered.lastIndex());
    assertEquals("cmd-1", recovered.get(1).orElseThrow().command());
    assertEquals("cmd-2", recovered.get(2).orElseThrow().command());
    assertEquals("cmd-3", recovered.get(3).orElseThrow().command());
  }

  @Test
  void successfulConflictingSuffixReplacementIsReflectedInMemoryImmediately() {
    Path file = tempDir.resolve("raft.log");
    DurableRaftLog log = new DurableRaftLog(new FileRaftLogStore(file));

    log.appendEntries(0, 0, List.of(new LogEntry(1, 1, "cmd-1"), new LogEntry(1, 2, "stale-2")));

    boolean accepted = log.appendEntries(1, 1, List.of(new LogEntry(2, 2, "cmd-2v2")));

    assertTrue(accepted);
    assertEquals(2, log.lastIndex());
    assertEquals("cmd-2v2", log.get(2).orElseThrow().command());
  }

  @Test
  void restartAfterFailedAppendThenSuccessfulAppendKeepsIndexesContiguous() {
    Path file = tempDir.resolve("raft.log");
    FailingRaftLogStore store = new FailingRaftLogStore(new FileRaftLogStore(file));
    DurableRaftLog log = new DurableRaftLog(store);

    log.appendCommand(1, "cmd-1");

    store.failNextReplace();
    assertThrows(RaftPersistenceException.class, () -> log.appendCommand(1, "cmd-2-lost"));

    // A retried append after the failure must resume at the next contiguous index, not skip one.
    log.appendCommand(1, "cmd-2-retry");

    DurableRaftLog recovered = new DurableRaftLog(new FileRaftLogStore(file));
    assertEquals(2, recovered.lastIndex());
    assertEquals("cmd-1", recovered.get(1).orElseThrow().command());
    assertEquals("cmd-2-retry", recovered.get(2).orElseThrow().command());
  }

  @Test
  void twoDurableLogInstancesOnDifferentFilesAreIndependent() {
    DurableRaftLog logA = new DurableRaftLog(new FileRaftLogStore(tempDir.resolve("a.log")));
    DurableRaftLog logB = new DurableRaftLog(new FileRaftLogStore(tempDir.resolve("b.log")));

    logA.appendCommand(1, "cmd-a");

    assertEquals(1, logA.lastIndex());
    assertEquals(0, logB.lastIndex());
  }

  // --- Real partial-write failures, using PartiallyFailingRaftLogStore ---------------------

  @Test
  void failureMidTempFileWriteLeavesTheOriginalDurableFileFullyRecoverable() {
    Path file = tempDir.resolve("raft.log");
    PartiallyFailingRaftLogStore store = new PartiallyFailingRaftLogStore(file);
    DurableRaftLog log = new DurableRaftLog(store);

    log.appendEntries(0, 0, List.of(new LogEntry(1, 1, "cmd-1"), new LogEntry(1, 2, "cmd-2")));

    store.failDuringNextTempWrite();
    assertThrows(
        RaftPersistenceException.class,
        () ->
            log.appendEntries(
                2, 1, List.of(new LogEntry(1, 3, "cmd-3"), new LogEntry(1, 4, "cmd-4"))));

    // Real bytes landed in the temp file, but the real file was never touched - it must still
    // hold exactly the pre-failure baseline, not a prefix of the failed batch.
    assertEquals(2, log.lastIndex());
    assertEquals("cmd-2", log.get(2).orElseThrow().command());

    DurableRaftLog recovered = new DurableRaftLog(new FileRaftLogStore(file));
    assertEquals(2, recovered.lastIndex());
    assertEquals("cmd-1", recovered.get(1).orElseThrow().command());
    assertEquals("cmd-2", recovered.get(2).orElseThrow().command());
  }

  @Test
  void failureBeforeAtomicReplacementLeavesTheOriginalDurableFileFullyRecoverable() {
    Path file = tempDir.resolve("raft.log");
    PartiallyFailingRaftLogStore store = new PartiallyFailingRaftLogStore(file);
    DurableRaftLog log = new DurableRaftLog(store);

    log.appendCommand(1, "cmd-1");

    store.failBeforeNextReplacement();
    assertThrows(RaftPersistenceException.class, () -> log.appendCommand(1, "cmd-2-never-lands"));

    // The temp file was fully and validly written (and forced) this time, but the rename that
    // would have promoted it over the real file never happened - the real file must be untouched.
    assertEquals(1, log.lastIndex());

    DurableRaftLog recovered = new DurableRaftLog(new FileRaftLogStore(file));
    assertEquals(1, recovered.lastIndex());
    assertEquals("cmd-1", recovered.get(1).orElseThrow().command());
  }

  @Test
  void inMemoryAndRecoveredDiskStateRemainIdenticalAfterAPartialWriteFailure() {
    Path file = tempDir.resolve("raft.log");
    PartiallyFailingRaftLogStore store = new PartiallyFailingRaftLogStore(file);
    DurableRaftLog log = new DurableRaftLog(store);

    log.appendCommand(1, "cmd-1");
    log.appendCommand(1, "cmd-2");

    store.failDuringNextTempWrite();
    assertThrows(RaftPersistenceException.class, () -> log.appendCommand(1, "cmd-3-never-lands"));

    DurableRaftLog recovered = new DurableRaftLog(new FileRaftLogStore(file));

    assertEquals(log.lastIndex(), recovered.lastIndex());
    for (long index = 1; index <= log.lastIndex(); index++) {
      assertEquals(log.get(index).orElseThrow(), recovered.get(index).orElseThrow());
    }
  }

  @Test
  void multiEntryAppendCannotRecoverAsOnlyAPrefixOfTheIntendedBatch() {
    // The specific failure this whole design exists to prevent: entry A of a multi-entry batch
    // durably landing while entry B does not. With a real partial write to the temp file (not just
    // a pre-invocation throw), the durable file must still show none of the failed batch - not
    // "cmd-2 but not cmd-3".
    Path file = tempDir.resolve("raft.log");
    PartiallyFailingRaftLogStore store = new PartiallyFailingRaftLogStore(file);
    DurableRaftLog log = new DurableRaftLog(store);

    log.appendCommand(1, "cmd-1"); // durable baseline

    store.failDuringNextTempWrite();
    assertThrows(
        RaftPersistenceException.class,
        () ->
            log.appendEntries(
                1, 1, List.of(new LogEntry(1, 2, "cmd-2"), new LogEntry(1, 3, "cmd-3"))));

    DurableRaftLog recovered = new DurableRaftLog(new FileRaftLogStore(file));
    assertEquals(1, recovered.lastIndex());
    assertEquals("cmd-1", recovered.get(1).orElseThrow().command());
  }

  @Test
  void restartAfterSuccessfulWholeLogReplacementRecoversExactly() {
    Path file = tempDir.resolve("raft.log");
    DurableRaftLog before = new DurableRaftLog(new FileRaftLogStore(file));

    before.appendCommand(1, "cmd-1");
    before.appendCommand(1, "cmd-2");
    before.appendEntries(2, 1, List.of(new LogEntry(1, 3, "cmd-3")));

    DurableRaftLog after = new DurableRaftLog(new FileRaftLogStore(file));
    assertEquals(3, after.lastIndex());
    assertEquals("cmd-1", after.get(1).orElseThrow().command());
    assertEquals("cmd-2", after.get(2).orElseThrow().command());
    assertEquals("cmd-3", after.get(3).orElseThrow().command());
  }

  @Test
  void restartAfterAFailedReplacementFollowedByASuccessfulOneRecoversOnlyTheSuccessfulState() {
    Path file = tempDir.resolve("raft.log");
    PartiallyFailingRaftLogStore store = new PartiallyFailingRaftLogStore(file);
    DurableRaftLog log = new DurableRaftLog(store);

    log.appendCommand(1, "cmd-1");

    store.failBeforeNextReplacement();
    assertThrows(RaftPersistenceException.class, () -> log.appendCommand(1, "cmd-2-lost"));

    // A stray, fully-written-but-never-promoted temp file from the failed attempt must not
    // interfere with the next successful replace() - it gets overwritten, not appended to.
    log.appendCommand(1, "cmd-2-retry");

    DurableRaftLog recovered = new DurableRaftLog(new FileRaftLogStore(file));
    assertEquals(2, recovered.lastIndex());
    assertEquals("cmd-1", recovered.get(1).orElseThrow().command());
    assertEquals("cmd-2-retry", recovered.get(2).orElseThrow().command());
  }
}
