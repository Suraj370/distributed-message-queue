package com.surajpanda.dmq.topic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.surajpanda.dmq.message.Message;
import com.surajpanda.dmq.partition.Partition;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TopicManagerMessageRecoveryTest {

  @TempDir Path tempDir;

  @Test
  void shouldRecoverPersistedMessagesAndNextOffsetAfterRestart() throws Exception {

    TopicManager firstManager = new TopicManager(tempDir);

    Topic topic = firstManager.createTopic("orders", 3);

    topic.getPartition(0).append("key-1", "order-created");
    topic.getPartition(0).append("key-2", "order-paid");

    TopicManager recoveredManager = new TopicManager(tempDir);

    Topic recoveredTopic = recoveredManager.getTopic("orders");

    assertNotNull(recoveredTopic);

    Partition recoveredPartition = recoveredTopic.getPartition(0);

    assertEquals(2, recoveredPartition.size());

    Message first = recoveredPartition.poll();
    Message second = recoveredPartition.poll();

    assertEquals("order-created", first.payload());
    assertEquals("order-paid", second.payload());

    Message third = recoveredPartition.append("key-3", "order-shipped");

    assertEquals(2, third.offset());
  }
}
