package com.surajpanda.dmq.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.surajpanda.dmq.message.Message;
import com.surajpanda.dmq.partition.Partition;
import com.surajpanda.dmq.wal.Wal;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ConsumerGroupOffsetTest {

  @TempDir Path tempDir;

  @Test
  void shouldHaveWellDefinedInitialCommittedOffset() throws Exception {

    Partition partition = new Partition(0, new Wal(tempDir.resolve("partition-0.log")));

    ConsumerGroup group = new ConsumerGroup("orders-group");

    assertEquals(0, group.getCommittedOffset(partition));
  }

  @Test
  void shouldCommitAndReadBackOffset() throws Exception {

    Partition partition = new Partition(0, new Wal(tempDir.resolve("partition-0.log")));

    ConsumerGroup group = new ConsumerGroup("orders-group");

    group.commitOffset(partition, 5);

    assertEquals(5, group.getCommittedOffset(partition));
  }

  @Test
  void shouldReplacePreviousOffsetWithNewerCommit() throws Exception {

    Partition partition = new Partition(0, new Wal(tempDir.resolve("partition-0.log")));

    ConsumerGroup group = new ConsumerGroup("orders-group");

    group.commitOffset(partition, 5);
    group.commitOffset(partition, 9);

    assertEquals(9, group.getCommittedOffset(partition));
  }

  @Test
  void shouldTrackIndependentOffsetsPerPartition() throws Exception {

    Partition firstPartition = new Partition(0, new Wal(tempDir.resolve("partition-0.log")));
    Partition secondPartition = new Partition(1, new Wal(tempDir.resolve("partition-1.log")));

    ConsumerGroup group = new ConsumerGroup("orders-group");

    group.commitOffset(firstPartition, 3);
    group.commitOffset(secondPartition, 7);

    assertEquals(3, group.getCommittedOffset(firstPartition));
    assertEquals(7, group.getCommittedOffset(secondPartition));
  }

  @Test
  void shouldTrackIndependentOffsetsPerConsumerGroup() throws Exception {

    Partition partition = new Partition(0, new Wal(tempDir.resolve("partition-0.log")));

    ConsumerGroup firstGroup = new ConsumerGroup("orders-group");
    ConsumerGroup secondGroup = new ConsumerGroup("billing-group");

    firstGroup.commitOffset(partition, 4);
    secondGroup.commitOffset(partition, 1);

    assertEquals(4, firstGroup.getCommittedOffset(partition));
    assertEquals(1, secondGroup.getCommittedOffset(partition));
  }

  @Test
  void pollingShouldNotAutomaticallyChangeCommittedOffset() throws Exception {

    Partition partition = new Partition(0, new Wal(tempDir.resolve("partition-0.log")));

    partition.append("key-1", "message-1");
    partition.append("key-2", "message-2");

    Consumer consumer = new Consumer(partition);

    ConsumerGroup group = new ConsumerGroup("orders-group");
    group.addConsumer(consumer);
    group.assign(partition, consumer);

    Message first = consumer.poll();
    Message second = consumer.poll();

    assertEquals("message-1", first.payload());
    assertEquals("message-2", second.payload());

    assertEquals(0, group.getCommittedOffset(partition));
  }

  @Test
  void shouldRejectNegativeCommittedOffset() throws Exception {

    Partition partition = new Partition(0, new Wal(tempDir.resolve("partition-0.log")));

    ConsumerGroup group = new ConsumerGroup("orders-group");

    assertThrows(IllegalArgumentException.class, () -> group.commitOffset(partition, -1));
  }
}
