package com.surajpanda.dmq.consumer;

import com.surajpanda.dmq.partition.Partition;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ConsumerGroup {

  private static final long INITIAL_COMMITTED_OFFSET = 0L;

  private final String groupId;

  private final Set<Consumer> members = ConcurrentHashMap.newKeySet();

  private final Map<Partition, Consumer> assignments = new ConcurrentHashMap<>();

  private final Map<Partition, Long> committedOffsets = new ConcurrentHashMap<>();

  public ConsumerGroup(String groupId) {
    this.groupId = groupId;
  }

  public String getGroupId() {
    return groupId;
  }

  public void addConsumer(Consumer consumer) {
    members.add(consumer);
  }

  public int memberCount() {
    return members.size();
  }

  public void assign(Partition partition, Consumer consumer) {

    if (!members.contains(consumer)) {
      throw new IllegalArgumentException("Consumer is not a member of group: " + groupId);
    }

    assignments.put(partition, consumer);
  }

  public Consumer getConsumer(Partition partition) {
    return assignments.get(partition);
  }

  public long getCommittedOffset(Partition partition) {
    return committedOffsets.getOrDefault(partition, INITIAL_COMMITTED_OFFSET);
  }

  public void commitOffset(Partition partition, long offset) {

    if (offset < 0) {
      throw new IllegalArgumentException("Committed offset must not be negative: " + offset);
    }

    committedOffsets.put(partition, offset);
  }
}
