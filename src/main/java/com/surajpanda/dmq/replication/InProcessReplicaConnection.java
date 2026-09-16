package com.surajpanda.dmq.replication;

import com.surajpanda.dmq.message.Message;
import com.surajpanda.dmq.partition.Partition;
import java.io.IOException;

public class InProcessReplicaConnection implements ReplicaConnection {

  private final Partition replicaPartition;

  public InProcessReplicaConnection(Partition replicaPartition) {
    this.replicaPartition = replicaPartition;
  }

  @Override
  public ReplicationOutcome send(Message message) {

    try {
      replicaPartition.applyReplicated(message);
      return ReplicationOutcome.ok();
    } catch (IOException exception) {
      return ReplicationOutcome.failed(exception);
    }
  }
}
