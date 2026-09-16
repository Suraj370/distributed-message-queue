package com.surajpanda.dmq.partition;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.surajpanda.dmq.message.Message;
import com.surajpanda.dmq.wal.Wal;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PartitionConsumerTest {

  @TempDir Path tempDir;

  @Test
  void shouldPollAppendedMessagesInOrderWithSequentialOffsets() throws Exception {

    Path walFile = tempDir.resolve("partition-0.log");

    Wal wal = new Wal(walFile);

    Partition partition = new Partition(0, wal);

    partition.append("key-1", "message-1");
    partition.append("key-2", "message-2");
    partition.append("key-3", "message-3");

    Message first = partition.poll();
    Message second = partition.poll();
    Message third = partition.poll();
    Message fourth = partition.poll();

    assertEquals("message-1", first.payload());
    assertEquals("message-2", second.payload());
    assertEquals("message-3", third.payload());
    assertNull(fourth);

    assertEquals(0, first.offset());
    assertEquals(1, second.offset());
    assertEquals(2, third.offset());
  }
}
