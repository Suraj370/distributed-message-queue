package com.surajpanda.dmq.raft;

/**
 * A pluggable target for committed Raft log entries. Deliberately not the queue's Partition/WAL -
 * how (or whether) a committed Raft entry maps onto durable queue state is a decision for a later
 * milestone. This interface exists so that decision can be made without redesigning RaftLogApplier
 * or RaftNode's commit/apply bookkeeping.
 */
public interface RaftStateMachine {

  void apply(LogEntry entry);
}
