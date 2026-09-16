package com.surajpanda.dmq.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class BrokerPropertiesTest {

  private final ApplicationContextRunner contextRunner =
      new ApplicationContextRunner().withUserConfiguration(TestConfig.class);

  @EnableConfigurationProperties(BrokerProperties.class)
  static class TestConfig {}

  @Test
  void shouldLoadBrokerIdFromConfiguration() {
    contextRunner
        .withPropertyValues(
            "broker.id=broker-1",
            "broker.host=localhost",
            "broker.port=8080",
            "broker.data-directory=data/broker-1")
        .run(
            context -> {
              BrokerProperties properties = context.getBean(BrokerProperties.class);
              assertEquals("broker-1", properties.id());
            });
  }

  @Test
  void shouldLoadHostAndPortFromConfiguration() {
    contextRunner
        .withPropertyValues(
            "broker.id=broker-1",
            "broker.host=localhost",
            "broker.port=8080",
            "broker.data-directory=data/broker-1")
        .run(
            context -> {
              BrokerProperties properties = context.getBean(BrokerProperties.class);
              assertEquals("localhost", properties.host());
              assertEquals(8080, properties.port());
            });
  }

  @Test
  void shouldLoadDataDirectoryFromConfiguration() {
    contextRunner
        .withPropertyValues(
            "broker.id=broker-1",
            "broker.host=localhost",
            "broker.port=8080",
            "broker.data-directory=data/broker-1")
        .run(
            context -> {
              BrokerProperties properties = context.getBean(BrokerProperties.class);
              assertEquals("data/broker-1", properties.dataDirectory());
            });
  }

  @Test
  void shouldRejectBlankBrokerId() {
    contextRunner
        .withPropertyValues(
            "broker.id=",
            "broker.host=localhost",
            "broker.port=8080",
            "broker.data-directory=data/broker-1")
        .run(context -> assertNotNull(context.getStartupFailure()));
  }

  @Test
  void shouldRejectBlankHost() {
    contextRunner
        .withPropertyValues(
            "broker.id=broker-1",
            "broker.host=",
            "broker.port=8080",
            "broker.data-directory=data/broker-1")
        .run(context -> assertNotNull(context.getStartupFailure()));
  }

  @Test
  void shouldRejectInvalidPort() {
    contextRunner
        .withPropertyValues(
            "broker.id=broker-1",
            "broker.host=localhost",
            "broker.port=0",
            "broker.data-directory=data/broker-1")
        .run(context -> assertNotNull(context.getStartupFailure()));
  }

  @Test
  void shouldRejectBlankDataDirectory() {
    contextRunner
        .withPropertyValues(
            "broker.id=broker-1",
            "broker.host=localhost",
            "broker.port=8080",
            "broker.data-directory=")
        .run(context -> assertNotNull(context.getStartupFailure()));
  }
}
