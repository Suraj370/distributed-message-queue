package com.surajpanda.dmq.broker;

import com.surajpanda.dmq.message.Message;
import com.surajpanda.dmq.queue.NotLeaderException;
import com.surajpanda.dmq.queue.PublishCommand;
import com.surajpanda.dmq.queue.QuorumUnavailableException;
import com.surajpanda.dmq.queue.RaftReplicatedQueue;
import com.surajpanda.dmq.topic.Topic;
import com.surajpanda.dmq.topic.TopicManager;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class Broker {

  private final TopicManager topicManager;
  private final BrokerProperties brokerProperties;
  private final RaftReplicatedQueue replicatedQueue;
  private final MeterRegistry meterRegistry;

  public Broker(
      TopicManager topicManager,
      BrokerProperties brokerProperties,
      RaftReplicatedQueue replicatedQueue) {
    this(topicManager, brokerProperties, replicatedQueue, new SimpleMeterRegistry());
  }

  /**
   * Same as the three-arg constructor, but records publish activity/latency/errors into
   * meterRegistry (see {@link #publish}). The three-arg constructor defaults to a throwaway {@link
   * SimpleMeterRegistry} so every existing caller (tests included) keeps working exactly as before;
   * only the real Spring-wired broker (see BrokerConfiguration) needs the metrics to actually go
   * anywhere.
   */
  @Autowired
  public Broker(
      TopicManager topicManager,
      BrokerProperties brokerProperties,
      RaftReplicatedQueue replicatedQueue,
      MeterRegistry meterRegistry) {
    this.topicManager = topicManager;
    this.brokerProperties = brokerProperties;
    this.replicatedQueue = replicatedQueue;
    this.meterRegistry = meterRegistry;
  }

  public String getBrokerId() {
    return brokerProperties.id();
  }

  public Topic createTopic(String name, int partitionCount) throws IOException {
    return topicManager.createTopic(name, partitionCount);
  }

  /**
   * Publishes via Raft: the message only becomes visible in queue state once the corresponding
   * PublishCommand has been committed by a majority of the configured Raft cluster and applied by
   * the queue state machine - see RaftReplicatedQueue. Throws
   * com.surajpanda.dmq.queue.NotLeaderException if this broker is not the Raft leader, or
   * com.surajpanda.dmq.queue.QuorumUnavailableException if a majority could not be reached.
   */
  public Message publish(String topicName, String key, String payload) {

    Timer.Sample publishTiming = Timer.start(meterRegistry);
    try {
      Topic topic = topicManager.getTopic(topicName);

      if (topic == null) {
        meterRegistry
            .counter("dmq_publish_errors_total", "topic", topicName, "reason", "topic_not_found")
            .increment();
        throw new IllegalArgumentException("Topic does not exist: " + topicName);
      }

      int partitionId = PartitionSelector.select(key.hashCode(), topic.getPartitions().size());

      try {
        Message message =
            replicatedQueue.propose(new PublishCommand(topicName, partitionId, key, payload));
        meterRegistry
            .counter(
                "dmq_messages_published_total",
                "topic",
                topicName,
                "partition",
                String.valueOf(partitionId))
            .increment();
        return message;
      } catch (NotLeaderException | QuorumUnavailableException exception) {
        meterRegistry
            .counter(
                "dmq_publish_errors_total",
                "topic",
                topicName,
                "reason",
                exception.getClass().getSimpleName())
            .increment();
        throw exception;
      }
    } finally {
      publishTiming.stop(
          Timer.builder("dmq_publish_latency_seconds")
              .description("End-to-end latency of Broker.publish(), Raft commit included")
              .tag("topic", topicName)
              .register(meterRegistry));
    }
  }

  /**
   * Non-destructive, offset-indexed read of this broker's own local queue state - added so an
   * external caller (an operator, or a test driving the real HTTP API) can verify a committed
   * publish actually reached queue state without reaching into this broker's internal Java objects.
   * Empty if the topic, partition, or offset does not exist; never throws for those cases.
   */
  public Optional<Message> read(String topicName, int partitionId, long offset) {

    Topic topic = topicManager.getTopic(topicName);

    if (topic == null || partitionId < 0 || partitionId >= topic.getPartitions().size()) {
      return Optional.empty();
    }

    return topic.getPartition(partitionId).get(offset);
  }
}
