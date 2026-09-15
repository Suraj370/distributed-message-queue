package com.surajpanda.dmq.topic;

import com.surajpanda.dmq.partition.Partition;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class TopicManager {

    private final Map<String, Topic> topics = new ConcurrentHashMap<>();

    public Topic createTopic(String name, int partitionCount) {

        if (partitionCount <= 0) {
            throw new IllegalArgumentException(
                    "Partition count must be greater than zero"
            );
        }

        if (topics.containsKey(name)) {
            throw new IllegalArgumentException(
                    "Topic already exists: " + name
            );
        }

        var partitions = new ArrayList<Partition>();

        for (int i = 0; i < partitionCount; i++) {
            partitions.add(new Partition(i));
        }

        Topic topic = new Topic(name, partitions);

        topics.put(name, topic);

        return topic;
    }

    public Topic getTopic(String name) {
        return topics.get(name);
    }
}