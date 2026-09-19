package com.surajpanda.dmq.consumer;

import com.surajpanda.dmq.partition.Partition;

/**
 * A stable, restart-safe identity for a partition (topic name + partition id), used as a
 * durable-storage key - unlike a Partition object reference, this survives a process restart.
 */
public record PartitionKey(String topic, int partitionId) {

  public static PartitionKey of(Partition partition) {
    return new PartitionKey(partition.getTopicName(), partition.getId());
  }
}
