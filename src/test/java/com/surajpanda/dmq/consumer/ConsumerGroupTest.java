package com.surajpanda.dmq.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.surajpanda.dmq.partition.Partition;
import com.surajpanda.dmq.wal.Wal;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ConsumerGroupTest {

  @TempDir Path tempDir;

  @Test
  void shouldTrackWhichConsumerOwnsAnAssignedPartition() throws Exception {

    Path walFile = tempDir.resolve("partition-0.log");

    Wal wal = new Wal(walFile);

    Partition partition = new Partition(0, wal);

    Consumer consumer = new Consumer(partition);

    ConsumerGroup group = new ConsumerGroup("orders-group");

    group.addConsumer(consumer);

    group.assign(partition, consumer);

    assertEquals(consumer, group.getConsumer(partition));
  }
}
