package com.surajpanda.dmq.queue;

/**
 * Durable tracking of the highest Raft log index this state machine has already applied. This is
 * independent of RaftNode's own in-memory lastApplied: after a restart, RaftNode's commitIndex and
 * lastApplied reset to whatever the recovered durable log/metadata imply, but the *queue state
 * machine* needs its own durable record of "have I already called Partition.append() for this
 * index" so a restart followed by re-driving commitIndex up to a previously-applied index can never
 * duplicate a message into the WAL.
 */
public interface LastAppliedStore {

  void save(long lastAppliedIndex);

  long load();
}
