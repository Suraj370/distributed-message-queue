package com.surajpanda.dmq.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class ClusterPropertiesTest {

  private final ApplicationContextRunner contextRunner =
      new ApplicationContextRunner().withUserConfiguration(TestConfig.class);

  @EnableConfigurationProperties(ClusterProperties.class)
  static class TestConfig {}

  @Test
  void shouldRepresentMultipleBrokersInCluster() {
    contextRunner
        .withPropertyValues(
            "cluster.brokers[0].id=broker-1",
            "cluster.brokers[0].host=localhost",
            "cluster.brokers[0].port=8080",
            "cluster.brokers[1].id=broker-2",
            "cluster.brokers[1].host=localhost",
            "cluster.brokers[1].port=8081",
            "cluster.brokers[2].id=broker-3",
            "cluster.brokers[2].host=localhost",
            "cluster.brokers[2].port=8082")
        .run(
            context -> {
              ClusterProperties properties = context.getBean(ClusterProperties.class);
              List<BrokerAddress> brokers = properties.brokers();

              assertEquals(3, brokers.size());
              assertTrue(brokers.stream().anyMatch(broker -> broker.id().equals("broker-1")));
              assertTrue(brokers.stream().anyMatch(broker -> broker.id().equals("broker-2")));
              assertTrue(brokers.stream().anyMatch(broker -> broker.id().equals("broker-3")));
            });
  }

  @Test
  void shouldRejectDuplicateBrokerIds() {
    contextRunner
        .withPropertyValues(
            "cluster.brokers[0].id=broker-1",
            "cluster.brokers[0].host=localhost",
            "cluster.brokers[0].port=8080",
            "cluster.brokers[1].id=broker-1",
            "cluster.brokers[1].host=localhost",
            "cluster.brokers[1].port=8081")
        .run(context -> assertNotNull(context.getStartupFailure()));
  }

  @Test
  void shouldRejectEmptyClusterMembership() {
    contextRunner.run(context -> assertNotNull(context.getStartupFailure()));
  }

  @Test
  void shouldRejectBrokerAddressWithBlankId() {
    contextRunner
        .withPropertyValues(
            "cluster.brokers[0].id=",
            "cluster.brokers[0].host=localhost",
            "cluster.brokers[0].port=8080")
        .run(context -> assertNotNull(context.getStartupFailure()));
  }
}
