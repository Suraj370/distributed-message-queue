package com.surajpanda.dmq.raft;

/**
 * Default, non-durable RaftMetadataStore. This is what every existing RaftNode(nodeId) test
 * continues to use, so their behavior is unchanged: nothing survives past the RaftNode instance
 * itself.
 */
public class InMemoryRaftMetadataStore implements RaftMetadataStore {

  private volatile RaftMetadataSnapshot snapshot = RaftMetadataSnapshot.initial();

  @Override
  public void save(long currentTerm, String votedFor) {
    snapshot = new RaftMetadataSnapshot(currentTerm, votedFor);
  }

  @Override
  public RaftMetadataSnapshot load() {
    return snapshot;
  }
}
