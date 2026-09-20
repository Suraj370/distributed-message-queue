package com.surajpanda.dmq.message;

import java.time.Instant;
import java.util.UUID;

/**
 * raftLogIndex is null for a message not produced by applying a committed Raft command (e.g. a
 * direct/test append, or a replica-sync copy). When present, it identifies the Raft log entry whose
 * application produced this message, and is what RaftQueueStateMachine uses to detect that a
 * committed entry was already durably applied to this partition - see
 * Partition.hasAppliedRaftIndex.
 */
public record Message(
    UUID id,
    String key,
    String payload,
    Instant timestamp,
    int partition,
    long offset,
    Long raftLogIndex) {

  public static Message create(String key, String payload, int partition, long offset) {
    return create(key, payload, partition, offset, null);
  }

  public static Message create(
      String key, String payload, int partition, long offset, Long raftLogIndex) {
    return new Message(
        UUID.randomUUID(), key, payload, Instant.now(), partition, offset, raftLogIndex);
  }
}
