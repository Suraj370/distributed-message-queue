package com.surajpanda.dmq.broker;

import com.surajpanda.dmq.message.Message;
import com.surajpanda.dmq.queue.PublishCommand;
import com.surajpanda.dmq.queue.RaftReplicatedQueue;
import com.surajpanda.dmq.topic.Topic;
import com.surajpanda.dmq.topic.TopicManager;
import java.io.IOException;
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

    int partitionId = Math.abs(key.hashCode()) % topic.getPartitions().size();

    return replicatedQueue.propose(new PublishCommand(topicName, partitionId, key, payload));
  }
}
