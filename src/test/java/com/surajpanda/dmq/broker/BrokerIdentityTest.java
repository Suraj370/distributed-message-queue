package com.surajpanda.dmq.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.surajpanda.dmq.topic.TopicManager;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BrokerIdentityTest {

  @TempDir Path tempDir;

  @Test
  void shouldExposeItsOwnBrokerId() throws Exception {

    TopicManager topicManager = new TopicManager(tempDir);

    BrokerProperties brokerProperties =
        new BrokerProperties("broker-1", "localhost", 8080, tempDir.toString());

    Broker broker = new Broker(topicManager, brokerProperties);

    assertEquals("broker-1", broker.getBrokerId());
  }
}
