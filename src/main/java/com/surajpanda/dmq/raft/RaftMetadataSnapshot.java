package com.surajpanda.dmq.raft;

public record RaftMetadataSnapshot(long currentTerm, String votedFor) {

  public static RaftMetadataSnapshot initial() {
    return new RaftMetadataSnapshot(0, null);
  }
}
