package com.surajpanda.dmq.partition;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.surajpanda.dmq.message.Message;
import com.surajpanda.dmq.wal.Wal;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PartitionRecoveryTest {

  @TempDir Path tempDir;

  @Test
  void shouldRecoverMessagesAndNextOffsetFromWal() throws Exception {

    Path walFile = tempDir.resolve("partition-0.log");

    Wal wal = new Wal(walFile);

    Partition originalPartition = new Partition(0, wal);

    originalPartition.append("key-1", "message-1");
    originalPartition.append("key-2", "message-2");
    originalPartition.append("key-3", "message-3");

    Partition recoveredPartition = new Partition(0, new Wal(walFile));

    assertEquals(3, recoveredPartition.size());

    Message first = recoveredPartition.poll();
    Message second = recoveredPartition.poll();
    Message third = recoveredPartition.poll();

    assertEquals("message-1", first.payload());
    assertEquals("message-2", second.payload());
    assertEquals("message-3", third.payload());

    Message nextMessage = recoveredPartition.append("key-4", "message-4");

    assertEquals(3, nextMessage.offset());
  }
}
