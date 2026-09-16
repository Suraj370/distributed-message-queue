package com.surajpanda.dmq.broker;

import com.surajpanda.dmq.message.Message;
import com.surajpanda.dmq.topic.Topic;
import com.surajpanda.dmq.topic.TopicManager;
import java.io.IOException;
import org.springframework.stereotype.Service;

@Service
public class Broker {

  private final TopicManager topicManager;

  public Broker(TopicManager topicManager) {
    this.topicManager = topicManager;
  }

  public Topic createTopic(String name, int partitionCount) throws IOException {
    return topicManager.createTopic(name, partitionCount);
  }

  public Message publish(String topicName, String key, String payload) throws IOException {

    Topic topic = topicManager.getTopic(topicName);

    if (topic == null) {
      throw new IllegalArgumentException("Topic does not exist: " + topicName);
    }

    int partitionId = Math.abs(key.hashCode()) % topic.getPartitions().size();

    return topic.getPartition(partitionId).append(key, payload);
  }
}
