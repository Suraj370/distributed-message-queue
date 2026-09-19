package com.surajpanda.dmq.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
  void corruptedFinalLogRecordIsDiscardedOnRecovery() throws Exception {
    Path file = tempDir.resolve("raft.log");

    DurableRaftLog before = new DurableRaftLog(new FileRaftLogStore(file));
    before.appendCommand(1, "cmd-1");
    before.appendCommand(1, "cmd-2");

    // Simulate a crash mid-write: an incomplete final line appended directly to the file.
    Files.writeString(file, "E|3|1|not-complete", java.nio.file.StandardOpenOption.APPEND);

    DurableRaftLog after = new DurableRaftLog(new FileRaftLogStore(file));

    assertEquals(2, after.lastIndex());
    assertEquals("cmd-1", after.get(1).orElseThrow().command());
    assertEquals("cmd-2", after.get(2).orElseThrow().command());
  }

  @Test
  void corruptedRecordInTheMiddleOfTheFileFailsRecoveryLoudly() throws Exception {
    Path file = tempDir.resolve("raft.log");

    DurableRaftLog before = new DurableRaftLog(new FileRaftLogStore(file));
    before.appendCommand(1, "cmd-1");

    Files.writeString(
        file,
        "GARBAGE-NOT-A-VALID-RECORD" + System.lineSeparator(),
        java.nio.file.StandardOpenOption.APPEND);
    before.appendCommand(1, "cmd-2");

    assertThrows(
        RaftPersistenceException.class, () -> new DurableRaftLog(new FileRaftLogStore(file)));
  }

  @Test
  void twoDurableLogInstancesOnDifferentFilesAreIndependent() {
    DurableRaftLog logA = new DurableRaftLog(new FileRaftLogStore(tempDir.resolve("a.log")));
    DurableRaftLog logB = new DurableRaftLog(new FileRaftLogStore(tempDir.resolve("b.log")));

    logA.appendCommand(1, "cmd-a");

    assertEquals(1, logA.lastIndex());
    assertEquals(0, logB.lastIndex());
  }
}
