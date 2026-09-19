package com.surajpanda.dmq.consumer;

import com.surajpanda.dmq.partition.Partition;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ConsumerGroup {

  private static final long INITIAL_COMMITTED_OFFSET = 0L;

  private final String groupId;
  private final OffsetStore offsetStore;
  private final Map<PartitionKey, Long> recoveredOffsets;

  private final Set<Consumer> members = ConcurrentHashMap.newKeySet();

  private final Map<Partition, Consumer> assignments = new ConcurrentHashMap<>();

  private final Map<Partition, Long> committedOffsets = new ConcurrentHashMap<>();

  public ConsumerGroup(String groupId) {
    this(groupId, new InMemoryOffsetStore());
  }

  /**
   * Recovers this group's durably committed offsets (if any) from offsetStore, so a restart resumes
   * exactly where the group left off rather than replaying from the beginning.
   */
  public ConsumerGroup(String groupId, OffsetStore offsetStore) {
    this.groupId = groupId;
    this.offsetStore = offsetStore;
    this.recoveredOffsets = offsetStore.loadAll(groupId);
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
    return committedOffsets.computeIfAbsent(
        partition,
        p -> recoveredOffsets.getOrDefault(PartitionKey.of(p), INITIAL_COMMITTED_OFFSET));
  }

  /**
   * Commits the offset up to which this group has successfully finished processing for the given
   * partition. Deliberately a separate call from fetching/polling a message - see
   * Consumer.fetchNext(): reading a message never advances the offset by itself, only an explicit
   * commitOffset() does. This is what makes at-least-once semantics possible: a crash between fetch
   * and commit leaves the offset unmoved, so the same message is fetched again.
   */
  public void commitOffset(Partition partition, long offset) {

    if (offset < 0) {
      throw new IllegalArgumentException("Committed offset must not be negative: " + offset);
    }

    committedOffsets.merge(partition, offset, Math::max);
    offsetStore.save(groupId, PartitionKey.of(partition), getCommittedOffset(partition));
  }

  /**
   * How many messages this group has not yet committed past, for the given partition -
   * consumer-group lag, exposed for future observability/metrics.
   */
  public long lag(Partition partition) {
    return partition.nextOffset() - getCommittedOffset(partition);
  }
}
