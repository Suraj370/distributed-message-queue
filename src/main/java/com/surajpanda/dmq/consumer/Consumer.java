package com.surajpanda.dmq.consumer;

import com.surajpanda.dmq.message.Message;
import com.surajpanda.dmq.partition.Partition;
import java.util.Optional;

/**
 * Supports two distinct delivery semantics against the same partition - see the project README for
 * the full explanation:
 *
 * <p>AT-MOST-ONCE via poll(): destructively removes the next message from the partition's local
 * queue. If the caller crashes after poll() returns but before finishing whatever it does with the
 * message, that message is gone - it was already removed.
 *
 * <p>AT-LEAST-ONCE via fetchNext(group) + group.commitOffset(...): fetchNext() never removes or
 * advances anything - it only reads. The offset only moves when the caller explicitly commits it,
 * after processing succeeds. A crash between fetch and commit means the next fetchNext() call
 * returns the same message again.
 */
public class Consumer {

  private final Partition partition;

  public Consumer(Partition partition) {
    this.partition = partition;
  }

  public Message poll() {
    return partition.poll();
  }

  public Partition getPartition() {
    return partition;
  }

  /**
   * Returns the next message after this group's committed offset for this consumer's partition,
   * without consuming or advancing anything. Empty if the group is caught up.
   */
  public Optional<Message> fetchNext(ConsumerGroup group) {
    return partition.get(group.getCommittedOffset(partition));
  }
}
