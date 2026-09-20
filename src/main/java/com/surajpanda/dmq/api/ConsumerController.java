package com.surajpanda.dmq.api;

import com.surajpanda.dmq.consumer.Consumer;
import com.surajpanda.dmq.consumer.ConsumerGroup;
import com.surajpanda.dmq.consumer.ConsumerGroupRegistry;
import com.surajpanda.dmq.message.Message;
import com.surajpanda.dmq.partition.Partition;
import com.surajpanda.dmq.topic.Topic;
import com.surajpanda.dmq.topic.TopicManager;
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

  public ConsumerController(TopicManager topicManager, ConsumerGroupRegistry registry) {
    this.topicManager = topicManager;
    this.registry = registry;
  }

  @PostMapping("/{groupId}/fetch")
  public ResponseEntity<Message> fetch(
      @PathVariable String groupId, @RequestParam String topic, @RequestParam int partition) {

    Partition target = partitionOrNull(topic, partition);
    if (target == null) {
      return ResponseEntity.notFound().build();
    }

    ConsumerGroup group = registry.get(groupId);
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

    registry.get(groupId).commitOffset(target, offset);
    return ResponseEntity.ok().build();
  }

  private Partition partitionOrNull(String topicName, int partitionId) {
    Topic topic = topicManager.getTopic(topicName);
    if (topic == null || partitionId < 0 || partitionId >= topic.getPartitions().size()) {
      return null;
    }
    return topic.getPartition(partitionId);
  }
}
