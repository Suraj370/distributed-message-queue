package com.surajpanda.dmq.topic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TopicManagerRecoveryTest {

  @TempDir Path tempDir;

  @Test
  void shouldRecoverTopicDefinitionsAfterRestart() throws Exception {

    TopicManager firstManager = new TopicManager(tempDir);

    Topic topic = firstManager.createTopic("orders", 3);

    assertEquals("orders", topic.getName());
    assertEquals(3, topic.getPartitions().size());

    TopicManager recoveredManager = new TopicManager(tempDir);

    Topic recoveredTopic = recoveredManager.getTopic("orders");

    assertNotNull(recoveredTopic);
    assertEquals("orders", recoveredTopic.getName());
    assertEquals(3, recoveredTopic.getPartitions().size());
  }
}
