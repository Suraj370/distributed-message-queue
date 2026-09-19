package com.surajpanda.dmq.raft;

/**
 * Applies committed-but-not-yet-applied entries from a RaftNode's log to a RaftStateMachine, one
 * index at a time in strict order, advancing lastApplied as it goes. Calling applyCommitted()
 * repeatedly (e.g. after every commitIndex advancement) is safe and idempotent - it only ever walks
 * forward from the current lastApplied, so an already-applied entry is never re-applied.
 */
public class RaftLogApplier {

  private final RaftNode node;
  private final RaftStateMachine stateMachine;

  public RaftLogApplier(RaftNode node, RaftStateMachine stateMachine) {
    this.node = node;
    this.stateMachine = stateMachine;
  }

  public void applyCommitted() {

    while (node.getLastApplied() < node.getCommitIndex()) {

      long nextIndex = node.getLastApplied() + 1;

      LogEntry entry =
          node.getLog()
              .get(nextIndex)
              .orElseThrow(
                  () -> new IllegalStateException("Missing committed entry at index " + nextIndex));

      stateMachine.apply(entry);
      node.markApplied(nextIndex);
    }
  }
}
