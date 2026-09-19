package com.surajpanda.dmq.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class RaftLogApplierTest {

  private static final class RecordingStateMachine implements RaftStateMachine {
    private final List<LogEntry> applied = new ArrayList<>();

    @Override
    public void apply(LogEntry entry) {
      applied.add(entry);
    }
  }

  @Test
  void doesNotApplyUncommittedEntries() {
    RaftNode node = new RaftNode("broker-1");
    node.getLog().appendCommand(1, "cmd-1");
    node.getLog().appendCommand(1, "cmd-2"); // commitIndex stays 0 - nothing committed yet

    RecordingStateMachine stateMachine = new RecordingStateMachine();
    new RaftLogApplier(node, stateMachine).applyCommitted();

    assertTrue(stateMachine.applied.isEmpty());
    assertEquals(0, node.getLastApplied());
  }

  @Test
  void appliesCommittedEntriesInOrder() {
    RaftNode node = new RaftNode("broker-1");
    node.getLog().appendCommand(1, "cmd-1");
    node.getLog().appendCommand(1, "cmd-2");
    node.getLog().appendCommand(1, "cmd-3");
    node.advanceCommitIndex(3);

    RecordingStateMachine stateMachine = new RecordingStateMachine();
    new RaftLogApplier(node, stateMachine).applyCommitted();

    assertEquals(3, stateMachine.applied.size());
    assertEquals("cmd-1", stateMachine.applied.get(0).command());
    assertEquals("cmd-2", stateMachine.applied.get(1).command());
    assertEquals("cmd-3", stateMachine.applied.get(2).command());
    assertEquals(3, node.getLastApplied());
  }

  @Test
  void doesNotApplyAnEntryTwice() {
    RaftNode node = new RaftNode("broker-1");
    node.getLog().appendCommand(1, "cmd-1");
    node.advanceCommitIndex(1);

    RecordingStateMachine stateMachine = new RecordingStateMachine();
    RaftLogApplier applier = new RaftLogApplier(node, stateMachine);
    applier.applyCommitted();
    applier.applyCommitted(); // nothing new to apply

    assertEquals(1, stateMachine.applied.size());
  }

  @Test
  void appliesOnlyNewlyCommittedEntriesOnSubsequentCalls() {
    RaftNode node = new RaftNode("broker-1");
    node.getLog().appendCommand(1, "cmd-1");
    node.getLog().appendCommand(1, "cmd-2");
    node.advanceCommitIndex(1);

    RecordingStateMachine stateMachine = new RecordingStateMachine();
    RaftLogApplier applier = new RaftLogApplier(node, stateMachine);
    applier.applyCommitted();

    node.advanceCommitIndex(2);
    applier.applyCommitted();

    assertEquals(2, stateMachine.applied.size());
    assertEquals("cmd-1", stateMachine.applied.get(0).command());
    assertEquals("cmd-2", stateMachine.applied.get(1).command());
  }

  @Test
  void lastAppliedNeverExceedsCommitIndex() {
    RaftNode node = new RaftNode("broker-1");
    node.getLog().appendCommand(1, "cmd-1");
    node.getLog().appendCommand(1, "cmd-2");
    node.advanceCommitIndex(1);

    assertThrows(IllegalArgumentException.class, () -> node.markApplied(2));
  }
}
