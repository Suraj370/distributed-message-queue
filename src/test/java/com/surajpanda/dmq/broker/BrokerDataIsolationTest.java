package com.surajpanda.dmq.broker;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.surajpanda.dmq.topic.TopicManager;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BrokerDataIsolationTest {

  @TempDir Path tempDir;

  @Test
  void shouldIsolateDataDirectoriesAcrossDifferentBrokerConfigurations() throws Exception {

    BrokerProperties broker1Properties =
        new BrokerProperties("broker-1", "localhost", 8080, tempDir.resolve("broker-1").toString());

    BrokerProperties broker2Properties =
        new BrokerProperties("broker-2", "localhost", 8081, tempDir.resolve("broker-2").toString());

    assertNotEquals(broker1Properties.dataDirectory(), broker2Properties.dataDirectory());

    TopicManager broker1Topics = new TopicManager(Path.of(broker1Properties.dataDirectory()));
    TopicManager broker2Topics = new TopicManager(Path.of(broker2Properties.dataDirectory()));

    broker1Topics.createTopic("orders", 1);

    assertNotNull(broker1Topics.getTopic("orders"));
    assertNull(broker2Topics.getTopic("orders"));
  }
}
