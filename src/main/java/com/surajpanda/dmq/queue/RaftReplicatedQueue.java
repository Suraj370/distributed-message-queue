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
 *
 * <p>The leader check above is not atomic with the replicate() call that follows it - this node can
 * step down (e.g. handling a concurrent higher-term AppendEntries/RequestVote on another thread) in
 * between, in which case replicator.replicate() itself throws IllegalStateException. That is
 * translated back into NotLeaderException here rather than left to escape as a raw
 * IllegalStateException, since from a caller's perspective stepping down mid-propose is the same
 * outcome as not having been leader in the first place.
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

    try {
      replicator.replicate(raftNode.getCommitIndex());
    } catch (IllegalStateException exception) {
      throw new NotLeaderException(
          "This node stepped down while replicating (state=" + raftNode.getState() + ")");
    }

    if (raftNode.getCommitIndex() < entry.index()) {
      throw new QuorumUnavailableException(
          "PublishCommand at index "
              + entry.index()
              + " did not reach a majority of the"
              + " configured cluster");
    }

    // The round above replicated leaderCommit as it stood *before* this entry committed, so a
    // follower that just accepted the entry does not yet know it is committed. A second round
    // carries the now-advanced commitIndex so followers can apply it without waiting on a later
    // publish to carry that information. Unlike the first round, the entry is already confirmed
    // committed by this point, so a step-down here is not this call's failure to report - the
    // periodic scheduler tick will carry the same information to followers regardless.
    try {
      replicator.replicate(raftNode.getCommitIndex());
    } catch (IllegalStateException exception) {
      // Stepped down between the majority check above and this courtesy round - safe to ignore.
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
