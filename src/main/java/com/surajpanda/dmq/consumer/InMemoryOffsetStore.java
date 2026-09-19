package com.surajpanda.dmq.consumer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Default, non-durable OffsetStore. This is what every existing ConsumerGroup(groupId) test
 * continues to use, so their behavior is unchanged.
 */
public class InMemoryOffsetStore implements OffsetStore {

  private final Map<String, Map<PartitionKey, Long>> offsetsByGroup = new ConcurrentHashMap<>();

  @Override
  public void save(String groupId, PartitionKey partitionKey, long offset) {
    offsetsByGroup
        .computeIfAbsent(groupId, id -> new ConcurrentHashMap<>())
        .put(partitionKey, offset);
  }

  @Override
  public Map<PartitionKey, Long> loadAll(String groupId) {
    return Map.copyOf(offsetsByGroup.getOrDefault(groupId, Map.of()));
  }
}
