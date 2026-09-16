package com.surajpanda.dmq.replication;

import com.surajpanda.dmq.message.Message;
import java.util.List;

public record ReplicationReport(Message message, List<String> failedReplicaBrokerIds) {

  public boolean fullySucceeded() {
    return failedReplicaBrokerIds.isEmpty();
  }
}
