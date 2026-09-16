package com.surajpanda.dmq.consumer;

import com.surajpanda.dmq.partition.Partition;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ConsumerGroup {

  private final String groupId;

  private final Set<Consumer> members = ConcurrentHashMap.newKeySet();

  private final Map<Partition, Consumer> assignments = new ConcurrentHashMap<>();

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
}
