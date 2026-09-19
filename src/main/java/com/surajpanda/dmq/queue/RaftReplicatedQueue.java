package com.surajpanda.dmq.queue;

import com.surajpanda.dmq.message.Message;
import com.surajpanda.dmq.raft.LogEntry;
import com.surajpanda.dmq.raft.RaftLogApplier;
import com.surajpanda.dmq.raft.RaftLogReplicator;
import com.surajpanda.dmq.raft.RaftNode;
import com.surajpanda.dmq.raft.RaftState;

/**
 * The single entry point for turning a client publish request into committed queue state. A
 * PublishCommand only ever reaches Partition/WAL by being proposed through Raft, replicated to a
 * majority, and applied by RaftLogApplier - never by mutating a Partition directly. propose() is
 * synchronous: it does not return until the command is committed and applied, or throws
 * NotLeaderException / QuorumUnavailableException.
 *
 * <p>This does not implement Raft itself - it only orchestrates the already-completed RaftNode /
 * RaftLogReplicator / RaftLogApplier components in the same call/return style already used
 * throughout this codebase's Raft classes.
 */
public class RaftReplicatedQueue {

  private final RaftNode raftNode;
  private final RaftLogReplicator replicator;
  private final RaftLogApplier applier;
  private final RaftQueueStateMachine stateMachine;

  public RaftReplicatedQueue(
      RaftNode raftNode,
      RaftLogReplicator replicator,
      RaftLogApplier applier,
      RaftQueueStateMachine stateMachine) {
    this.raftNode = raftNode;
    this.replicator = replicator;
    this.applier = applier;
    this.stateMachine = stateMachine;
  }

  public Message propose(PublishCommand command) {

    if (raftNode.getState() != RaftState.LEADER) {
      throw new NotLeaderException(
          "This node is not the Raft leader (state=" + raftNode.getState() + ")");
    }

    LogEntry entry = raftNode.getLog().appendCommand(raftNode.getCurrentTerm(), command.encode());

    replicator.replicate(raftNode.getCommitIndex());

    if (raftNode.getCommitIndex() < entry.index()) {
      throw new QuorumUnavailableException(
          "PublishCommand at index "
              + entry.index()
              + " did not reach a majority of the"
              + " configured cluster");
    }

    applier.applyCommitted();

    return stateMachine
        .takeAppliedMessage(entry.index())
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Committed entry " + entry.index() + " was not applied as expected"));
  }
}
