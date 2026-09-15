package com.surajpanda.dmq.topic;

import com.surajpanda.dmq.partition.Partition;

import java.util.List;

public class Topic {

    private final String name;
    private final List<Partition> partitions;

    public Topic(String name, List<Partition> partitions) {
        this.name = name;
        this.partitions = partitions;
    }

    public String getName() {
        return name;
    }

    public List<Partition> getPartitions() {
        return partitions;
    }

    public Partition getPartition(int partitionId) {
        return partitions.get(partitionId);
    }
}