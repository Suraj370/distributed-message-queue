package com.surajpanda.dmq.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.surajpanda.dmq.message.Message;
import com.surajpanda.dmq.partition.Partition;
import com.surajpanda.dmq.wal.Wal;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Demonstrates the two delivery semantics this project actually implements - see
 * Consumer.fetchNext()/poll() and the README's Delivery Semantics section. Neither path claims
 * exactly-once.
 */
class DeliverySemanticsTest {

  @TempDir Path tempDir;

  @Test
  void atMostOnce_pollLosesTheMessageIfProcessingNeverHappensAfterRemoval() throws Exception {
    Partition partition = new Partition("orders", 0, new Wal(tempDir.resolve("p0.log")));
    partition.append("key-1", "message-1");

    Consumer consumer = new Consumer(partition);

    Message removed =
        consumer.poll(); // message is gone from the partition the instant this returns
    assertEquals("message-1", removed.payload());

    // Simulating "the process crashed right here, before it finished handling `removed`" - there
    // is no way to get this message back. poll() already removed it.
    assertNull(consumer.poll());
    assertEquals(0, partition.size());
  }

  @Test
  void atLeastOnce_crashBeforeCommitRedeliversTheSameMessage() throws Exception {
    Partition partition = new Partition("orders", 0, new Wal(tempDir.resolve("p0.log")));
    partition.append("key-1", "message-1");

    ConsumerGroup group = new ConsumerGroup("orders-group");
    Consumer consumer = new Consumer(partition);

    Message fetchedBeforeCrash = consumer.fetchNext(group).orElseThrow();
    // "Processing" happens here, then the process crashes before commitOffset() runs.

    // A fresh fetch (as a restarted/rebalanced consumer would do) sees the same message again,
    // because the offset was never committed.
    Message fetchedAfterRestart = consumer.fetchNext(group).orElseThrow();

    assertEquals(fetchedBeforeCrash.offset(), fetchedAfterRestart.offset());
    assertEquals("message-1", fetchedAfterRestart.payload());
    assertTrue(partition.get(0).isPresent()); // unlike poll(), the message was never removed
  }

  @Test
  void atLeastOnce_successfulCommitPreventsRedelivery() throws Exception {
    Partition partition = new Partition("orders", 0, new Wal(tempDir.resolve("p0.log")));
    partition.append("key-1", "message-1");
    partition.append("key-2", "message-2");

    ConsumerGroup group = new ConsumerGroup("orders-group");
    Consumer consumer = new Consumer(partition);

    Message first = consumer.fetchNext(group).orElseThrow();
    group.commitOffset(partition, first.offset() + 1); // processing succeeded, commit it

    Message next = consumer.fetchNext(group).orElseThrow();

    assertEquals("message-2", next.payload()); // moved on, not redelivered
  }
}
