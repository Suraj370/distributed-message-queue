package com.surajpanda.dmq.broker;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.surajpanda.dmq.topic.TopicManager;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class BrokerConfigurationTest {

  @TempDir Path tempDir;

  @EnableConfigurationProperties(BrokerProperties.class)
  static class PropertiesConfig {}

  @Test
  void shouldBuildTopicManagerUsingConfiguredBrokerDataDirectory() {

    Path dataDirectory = tempDir.resolve("broker-1");

    new ApplicationContextRunner()
        .withUserConfiguration(PropertiesConfig.class, BrokerConfiguration.class)
        .withPropertyValues(
            "broker.id=broker-1",
            "broker.host=localhost",
            "broker.port=8080",
            "broker.data-directory=" + dataDirectory)
        .run(
            context -> {
              TopicManager topicManager = context.getBean(TopicManager.class);

              topicManager.createTopic("orders", 1);

              assertTrue(Files.exists(dataDirectory.resolve("topics.meta")));
            });
  }
}
