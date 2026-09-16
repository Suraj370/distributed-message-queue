package com.surajpanda.dmq.replication;

import com.surajpanda.dmq.message.Message;
import java.util.ArrayList;
import java.util.List;

public class PartitionReplicator {

  private final List<ReplicaTarget> replicas;

  public PartitionReplicator(List<ReplicaTarget> replicas) {
    this.replicas = List.copyOf(replicas);
  }

  public ReplicationReport replicate(Message message) {

    List<String> failedReplicaBrokerIds = new ArrayList<>();

    for (ReplicaTarget replica : replicas) {

      ReplicationOutcome outcome = replica.connection().send(message);

      if (!outcome.success()) {
        failedReplicaBrokerIds.add(replica.brokerId());
      }
    }

    return new ReplicationReport(message, List.copyOf(failedReplicaBrokerIds));
  }
}
