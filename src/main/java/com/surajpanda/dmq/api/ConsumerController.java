package com.surajpanda.dmq.api;

import com.surajpanda.dmq.consumer.Consumer;
import com.surajpanda.dmq.consumer.ConsumerGroup;
import com.surajpanda.dmq.consumer.ConsumerGroupRegistry;
import com.surajpanda.dmq.message.Message;
import com.surajpanda.dmq.partition.Partition;
import com.surajpanda.dmq.topic.Topic;
import com.surajpanda.dmq.topic.TopicManager;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Thin HTTP facade over the existing Consumer/ConsumerGroup classes - added so at-least-once
 * consumption (fetch, then a separate commit) can be exercised and verified over the real HTTP API,
 * the same way a Testcontainers-style external test already exercises publish. Does not change
 * consumer-group semantics at all: fetch() never advances anything, commit() is the only thing that
 * does, exactly as Consumer.fetchNext()/ConsumerGroup.commitOffset() already document.
 */
@RestController
@RequestMapping("/api/v1/consumer-groups")
public class ConsumerController {

  private final TopicManager topicManager;
  private final ConsumerGroupRegistry registry;
  private final MeterRegistry meterRegistry;

  public ConsumerController(TopicManager topicManager, ConsumerGroupRegistry registry) {
    this(topicManager, registry, new SimpleMeterRegistry());
  }

  /**
   * Same as the two-arg constructor, but records consumer fetch/commit activity and registers live
   * lag/committed-offset gauges into meterRegistry. The two-arg constructor defaults to a throwaway
   * {@link SimpleMeterRegistry} so every existing caller (tests included) keeps working exactly as
   * before; only the real Spring-wired broker needs the metrics to actually go anywhere.
   */
  @Autowired
  public ConsumerController(
      TopicManager topicManager, ConsumerGroupRegistry registry, MeterRegistry meterRegistry) {
    this.topicManager = topicManager;
    this.registry = registry;
    this.meterRegistry = meterRegistry;
  }

  @PostMapping("/{groupId}/fetch")
  public ResponseEntity<Message> fetch(
      @PathVariable String groupId, @RequestParam String topic, @RequestParam int partition) {

    Partition target = partitionOrNull(topic, partition);
    if (target == null) {
      return ResponseEntity.notFound().build();
    }

    ConsumerGroup group = registry.get(groupId);
    meterRegistry
        .counter(
            "dmq_consumer_fetch_total",
            "consumer_group",
            groupId,
            "topic",
            topic,
            "partition",
            String.valueOf(partition))
        .increment();
    registerConsumerGaugesOnce(groupId, topic, partition, group, target);

    return new Consumer(target)
        .fetchNext(group)
        .map(ResponseEntity::ok)
        .orElseGet(() -> ResponseEntity.noContent().build());
  }

  @PostMapping("/{groupId}/commit")
  public ResponseEntity<Void> commit(
      @PathVariable String groupId,
      @RequestParam String topic,
      @RequestParam int partition,
      @RequestParam long offset) {

    Partition target = partitionOrNull(topic, partition);
    if (target == null) {
      return ResponseEntity.notFound().build();
    }

    ConsumerGroup group = registry.get(groupId);
    group.commitOffset(target, offset);
    meterRegistry
        .counter(
            "dmq_consumer_commits_total",
            "consumer_group",
            groupId,
            "topic",
            topic,
            "partition",
            String.valueOf(partition))
        .increment();
    registerConsumerGaugesOnce(groupId, topic, partition, group, target);

    return ResponseEntity.ok().build();
  }

  /**
   * Registers this (consumer_group, topic, partition) combination's lag and committed-offset gauges
   * the first time it is observed. {@code MeterRegistry.gauge(...)} is idempotent for a repeated
   * identical id/tags combination - it returns the already-registered gauge rather than duplicating
   * it - so calling this on every fetch/commit is safe; the gauges themselves read live state from
   * group/target at scrape time, not at registration time.
   */
  private void registerConsumerGaugesOnce(
      String groupId, String topic, int partition, ConsumerGroup group, Partition target) {
    Tags tags =
        Tags.of(
            "consumer_group", groupId,
            "topic", topic,
            "partition", String.valueOf(partition));
    meterRegistry.gauge("dmq_consumer_lag", tags, group, g -> g.lag(target));
    meterRegistry.gauge(
        "dmq_consumer_committed_offset", tags, group, g -> g.getCommittedOffset(target));
  }

  private Partition partitionOrNull(String topicName, int partitionId) {
    Topic topic = topicManager.getTopic(topicName);
    if (topic == null || partitionId < 0 || partitionId >= topic.getPartitions().size()) {
      return null;
    }
    return topic.getPartition(partitionId);
  }
}
