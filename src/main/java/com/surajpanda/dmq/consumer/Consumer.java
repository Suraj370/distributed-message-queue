package com.surajpanda.dmq.consumer;

import com.surajpanda.dmq.message.Message;
import com.surajpanda.dmq.partition.Partition;

public class Consumer {

  private final Partition partition;

  public Consumer(Partition partition) {
    this.partition = partition;
  }

  public Message poll() {
    return partition.poll();
  }
}
