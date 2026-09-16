package com.surajpanda.dmq.partition;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.surajpanda.dmq.message.Message;
import com.surajpanda.dmq.wal.Wal;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PartitionReplicationTest {

  @TempDir Path tempDir;

  @Test
  void shouldPreserveOriginalMessageFieldsWhenApplyingAReplicatedMessage() throws Exception {

    Partition primary = new Partition(0, new Wal(tempDir.resolve("primary.log")));
    Partition replica = new Partition(0, new Wal(tempDir.resolve("replica.log")));

    Message original = primary.append("key-1", "message-1");

    replica.applyReplicated(original);

    Message stored = replica.poll();

    assertEquals(original.id(), stored.id());
    assertEquals(original.key(), stored.key());
    assertEquals(original.payload(), stored.payload());
    assertEquals(original.timestamp(), stored.timestamp());
    assertEquals(original.partition(), stored.partition());
    assertEquals(original.offset(), stored.offset());
  }

  @Test
  void shouldNotGenerateANewOffsetForAReplicatedMessage() throws Exception {

    Partition primary = new Partition(0, new Wal(tempDir.resolve("primary.log")));
    Partition replica = new Partition(0, new Wal(tempDir.resolve("replica.log")));

    primary.append("key-1", "message-1");
    Message second = primary.append("key-2", "message-2");

    replica.applyReplicated(second);

    assertEquals(1, replica.poll().offset());
  }

  @Test
  void shouldPreserveOrderingWhenApplyingMultipleReplicatedMessages() throws Exception {

    Partition primary = new Partition(0, new Wal(tempDir.resolve("primary.log")));
    Partition replica = new Partition(0, new Wal(tempDir.resolve("replica.log")));

    Message first = primary.append("key-1", "message-1");
    Message second = primary.append("key-2", "message-2");
    Message third = primary.append("key-3", "message-3");

    replica.applyReplicated(first);
    replica.applyReplicated(second);
    replica.applyReplicated(third);

    assertEquals("message-1", replica.poll().payload());
    assertEquals("message-2", replica.poll().payload());
    assertEquals("message-3", replica.poll().payload());
  }

  @Test
  void shouldNotCreateDuplicateLogicalMessagesWhenReplicatedTwice() throws Exception {

    Partition primary = new Partition(0, new Wal(tempDir.resolve("primary.log")));
    Partition replica = new Partition(0, new Wal(tempDir.resolve("replica.log")));

    Message message = primary.append("key-1", "message-1");

    replica.applyReplicated(message);
    replica.applyReplicated(message);
    replica.applyReplicated(message);

    assertEquals(1, replica.size());
  }
}
