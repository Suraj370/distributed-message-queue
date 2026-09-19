package com.surajpanda.dmq.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.surajpanda.dmq.partition.Partition;
import com.surajpanda.dmq.wal.Wal;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ConsumerOffsetPersistenceTest {

  @TempDir Path tempDir;

  @Test
  void initialOffsetIsZeroWithNoPriorCommits() throws Exception {
    Partition partition = new Partition("orders", 0, new Wal(tempDir.resolve("p0.log")));

    ConsumerGroup group =
        new ConsumerGroup("orders-group", new FileOffsetStore(tempDir.resolve("offsets")));

    assertEquals(0, group.getCommittedOffset(partition));
  }

  @Test
  void committedOffsetSurvivesRestart() throws Exception {
    Partition partition = new Partition("orders", 0, new Wal(tempDir.resolve("p0.log")));
    Path offsetsDir = tempDir.resolve("offsets");

    ConsumerGroup before = new ConsumerGroup("orders-group", new FileOffsetStore(offsetsDir));
    before.commitOffset(partition, 5);

    ConsumerGroup after = new ConsumerGroup("orders-group", new FileOffsetStore(offsetsDir));

    assertEquals(5, after.getCommittedOffset(partition));
  }

  @Test
  void independentOffsetsPerConsumerGroupSurviveRestart() throws Exception {
    Partition partition = new Partition("orders", 0, new Wal(tempDir.resolve("p0.log")));
    Path offsetsDir = tempDir.resolve("offsets");

    new ConsumerGroup("orders-group", new FileOffsetStore(offsetsDir)).commitOffset(partition, 4);
    new ConsumerGroup("billing-group", new FileOffsetStore(offsetsDir)).commitOffset(partition, 1);

    ConsumerGroup ordersAfter = new ConsumerGroup("orders-group", new FileOffsetStore(offsetsDir));
    ConsumerGroup billingAfter =
        new ConsumerGroup("billing-group", new FileOffsetStore(offsetsDir));

    assertEquals(4, ordersAfter.getCommittedOffset(partition));
    assertEquals(1, billingAfter.getCommittedOffset(partition));
  }

  @Test
  void independentOffsetsPerPartitionSurviveRestart() throws Exception {
    Partition first = new Partition("orders", 0, new Wal(tempDir.resolve("p0.log")));
    Partition second = new Partition("orders", 1, new Wal(tempDir.resolve("p1.log")));
    Path offsetsDir = tempDir.resolve("offsets");

    ConsumerGroup before = new ConsumerGroup("orders-group", new FileOffsetStore(offsetsDir));
    before.commitOffset(first, 3);
    before.commitOffset(second, 7);

    ConsumerGroup after = new ConsumerGroup("orders-group", new FileOffsetStore(offsetsDir));

    assertEquals(3, after.getCommittedOffset(first));
    assertEquals(7, after.getCommittedOffset(second));
  }

  @Test
  void replayHappensWhenProcessingCrashesBeforeOffsetCommit() throws Exception {
    Partition partition = new Partition("orders", 0, new Wal(tempDir.resolve("p0.log")));
    partition.append("key-1", "message-1");

    ConsumerGroup group =
        new ConsumerGroup("orders-group", new FileOffsetStore(tempDir.resolve("offsets")));
    Consumer consumer = new Consumer(partition);

    var first = consumer.fetchNext(group).orElseThrow();
    // "Processing" happens here but the process crashes before commitOffset() is ever called.

    var second = consumer.fetchNext(group).orElseThrow();

    assertEquals(first.offset(), second.offset());
    assertEquals("message-1", second.payload());
  }

  @Test
  void noReplayAfterSuccessfulOffsetCommit() throws Exception {
    Partition partition = new Partition("orders", 0, new Wal(tempDir.resolve("p0.log")));
    partition.append("key-1", "message-1");
    partition.append("key-2", "message-2");

    ConsumerGroup group =
        new ConsumerGroup("orders-group", new FileOffsetStore(tempDir.resolve("offsets")));
    Consumer consumer = new Consumer(partition);

    var first = consumer.fetchNext(group).orElseThrow();
    group.commitOffset(partition, first.offset() + 1);

    var second = consumer.fetchNext(group).orElseThrow();

    assertEquals("message-2", second.payload());
  }

  @Test
  void rejectsInvalidNegativeOffset() throws Exception {
    Partition partition = new Partition("orders", 0, new Wal(tempDir.resolve("p0.log")));
    ConsumerGroup group =
        new ConsumerGroup("orders-group", new FileOffsetStore(tempDir.resolve("offsets")));

    assertThrows(IllegalArgumentException.class, () -> group.commitOffset(partition, -1));
  }

  @Test
  void offsetMustNotMoveBackwardsAccidentally() throws Exception {
    Partition partition = new Partition("orders", 0, new Wal(tempDir.resolve("p0.log")));
    ConsumerGroup group =
        new ConsumerGroup("orders-group", new FileOffsetStore(tempDir.resolve("offsets")));

    group.commitOffset(partition, 10);
    group.commitOffset(partition, 3); // e.g. a stale/duplicate commit arriving out of order

    assertEquals(10, group.getCommittedOffset(partition));
  }
}
