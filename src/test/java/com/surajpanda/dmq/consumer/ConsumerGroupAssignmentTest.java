package com.surajpanda.dmq.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.surajpanda.dmq.partition.Partition;
import com.surajpanda.dmq.wal.Wal;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ConsumerGroupAssignmentTest {

  @TempDir Path tempDir;

  @Test
  void shouldRejectAssigningConsumerNotAddedToGroup() throws Exception {

    Partition partition = new Partition(0, new Wal(tempDir.resolve("partition-0.log")));

    Consumer consumer = new Consumer(partition);

    ConsumerGroup group = new ConsumerGroup("orders-group");

    assertThrows(IllegalArgumentException.class, () -> group.assign(partition, consumer));
  }

  @Test
  void shouldReassignPartitionToAtMostOneConsumer() throws Exception {

    Partition partition = new Partition(0, new Wal(tempDir.resolve("partition-0.log")));

    Consumer firstConsumer = new Consumer(partition);
    Consumer secondConsumer = new Consumer(partition);

    ConsumerGroup group = new ConsumerGroup("orders-group");

    group.addConsumer(firstConsumer);
    group.addConsumer(secondConsumer);

    group.assign(partition, firstConsumer);
    assertEquals(firstConsumer, group.getConsumer(partition));

    group.assign(partition, secondConsumer);
    assertEquals(secondConsumer, group.getConsumer(partition));
  }

  @Test
  void shouldAllowAConsumerToOwnMultiplePartitions() throws Exception {

    Partition firstPartition = new Partition(0, new Wal(tempDir.resolve("partition-0.log")));
    Partition secondPartition = new Partition(1, new Wal(tempDir.resolve("partition-1.log")));

    Consumer consumer = new Consumer(firstPartition);

    ConsumerGroup group = new ConsumerGroup("orders-group");

    group.addConsumer(consumer);

    group.assign(firstPartition, consumer);
    group.assign(secondPartition, consumer);

    assertEquals(consumer, group.getConsumer(firstPartition));
    assertEquals(consumer, group.getConsumer(secondPartition));
  }

  @Test
  void shouldNotCreateDuplicateMembershipForSameConsumer() throws Exception {

    Partition partition = new Partition(0, new Wal(tempDir.resolve("partition-0.log")));

    Consumer consumer = new Consumer(partition);

    ConsumerGroup group = new ConsumerGroup("orders-group");

    group.addConsumer(consumer);
    group.addConsumer(consumer);
    group.addConsumer(consumer);

    assertEquals(1, group.memberCount());
  }

  @Test
  void shouldBeIdempotentWhenAssigningSamePartitionToSameConsumer() throws Exception {

    Partition partition = new Partition(0, new Wal(tempDir.resolve("partition-0.log")));

    Consumer consumer = new Consumer(partition);

    ConsumerGroup group = new ConsumerGroup("orders-group");

    group.addConsumer(consumer);

    group.assign(partition, consumer);
    group.assign(partition, consumer);

    assertEquals(consumer, group.getConsumer(partition));
  }

  @Test
  void shouldAssignMultiplePartitionsToDifferentConsumersIndependently() throws Exception {

    Partition firstPartition = new Partition(0, new Wal(tempDir.resolve("partition-0.log")));
    Partition secondPartition = new Partition(1, new Wal(tempDir.resolve("partition-1.log")));

    Consumer firstConsumer = new Consumer(firstPartition);
    Consumer secondConsumer = new Consumer(secondPartition);

    ConsumerGroup group = new ConsumerGroup("orders-group");

    group.addConsumer(firstConsumer);
    group.addConsumer(secondConsumer);

    group.assign(firstPartition, firstConsumer);
    group.assign(secondPartition, secondConsumer);

    assertEquals(firstConsumer, group.getConsumer(firstPartition));
    assertEquals(secondConsumer, group.getConsumer(secondPartition));
  }
}
