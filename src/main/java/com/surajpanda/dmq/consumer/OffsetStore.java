package com.surajpanda.dmq.consumer;

import java.util.Map;

/**
 * Durable storage for consumer-group committed offsets, keyed by (groupId, topic, partitionId)
 * rather than by Partition object identity so offsets survive a restart.
 */
public interface OffsetStore {

  void save(String groupId, PartitionKey partitionKey, long offset);

  Map<PartitionKey, Long> loadAll(String groupId);
}
