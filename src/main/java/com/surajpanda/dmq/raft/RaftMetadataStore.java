package com.surajpanda.dmq.raft;

/**
 * Durable storage for the two pieces of Raft state that must survive a restart: currentTerm and
 * votedFor. Log entries and commitIndex/lastApplied are handled by separate stores - see
 * RaftLogStore and the queue state machine's own applied-index tracking.
 */
public interface RaftMetadataStore {

  void save(long currentTerm, String votedFor);

  RaftMetadataSnapshot load();
}
