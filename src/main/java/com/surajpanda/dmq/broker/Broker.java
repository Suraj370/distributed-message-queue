package com.surajpanda.dmq.broker;

import com.surajpanda.dmq.message.Message;
import com.surajpanda.dmq.queue.PublishCommand;
import com.surajpanda.dmq.queue.RaftReplicatedQueue;
import com.surajpanda.dmq.topic.Topic;
import com.surajpanda.dmq.topic.TopicManager;
import java.io.IOException;
import java.util.Optional;
import org.springframework.stereotype.Service;

@Service
public class Broker {

  private final TopicManager topicManager;
  private final BrokerProperties brokerProperties;
  private final RaftReplicatedQueue replicatedQueue;

  public Broker(
      TopicManager topicManager,
      BrokerProperties brokerProperties,
      RaftReplicatedQueue replicatedQueue) {
    this.topicManager = topicManager;
    this.brokerProperties = brokerProperties;
    this.replicatedQueue = replicatedQueue;
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

    Topic topic = topicManager.getTopic(topicName);

    if (topic == null) {
      throw new IllegalArgumentException("Topic does not exist: " + topicName);
    }

    int partitionId = PartitionSelector.select(key.hashCode(), topic.getPartitions().size());

    return replicatedQueue.propose(new PublishCommand(topicName, partitionId, key, payload));
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
