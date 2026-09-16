package com.surajpanda.dmq.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.surajpanda.dmq.message.Message;
import com.surajpanda.dmq.partition.Partition;
import com.surajpanda.dmq.wal.Wal;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ConsumerTest {

  @TempDir Path tempDir;

  @Test
  void shouldPollMessagesFromPartitionInOrder() throws Exception {

    Path walFile = tempDir.resolve("partition-0.log");

    Wal wal = new Wal(walFile);

    Partition partition = new Partition(0, wal);

    partition.append("key-1", "message-1");
    partition.append("key-2", "message-2");

    Consumer consumer = new Consumer(partition);

    Message first = consumer.poll();
    Message second = consumer.poll();
    Message third = consumer.poll();

    assertEquals("message-1", first.payload());
    assertEquals("message-2", second.payload());
    assertNull(third);
  }
}
