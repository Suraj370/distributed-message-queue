package com.surajpanda.dmq.consumer;

import com.surajpanda.dmq.broker.BrokerProperties;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;

/**
 * Lazily creates and caches one durable ConsumerGroup per groupId, backed by a FileOffsetStore
 * under this broker's own data directory - the same durability story FileOffsetStore already has
 * (see its own javadoc), just wired up as a broker-wide registry so the consumer-facing HTTP API
 * (ConsumerController) can look a group up by name instead of every caller constructing its own.
 * Does not change ConsumerGroup/OffsetStore behavior at all.
 */
@Component
public class ConsumerGroupRegistry {

  private final Map<String, ConsumerGroup> groups = new ConcurrentHashMap<>();
  private final Path offsetsDirectory;

  public ConsumerGroupRegistry(BrokerProperties brokerProperties) {
    this.offsetsDirectory = Path.of(brokerProperties.dataDirectory()).resolve("offsets");
  }

  public ConsumerGroup get(String groupId) {
    return groups.computeIfAbsent(
        groupId, id -> new ConsumerGroup(id, new FileOffsetStore(offsetsDirectory)));
  }
}
