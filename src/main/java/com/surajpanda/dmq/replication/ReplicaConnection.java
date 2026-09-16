package com.surajpanda.dmq.replication;

import com.surajpanda.dmq.message.Message;

public interface ReplicaConnection {

  ReplicationOutcome send(Message message);
}
