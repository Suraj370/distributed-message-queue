package com.surajpanda.dmq.partition;

import com.surajpanda.dmq.message.Message;
import com.surajpanda.dmq.wal.Wal;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class Partition {

  private static final String UNKNOWN_TOPIC = "unknown";

  private final String topicName;
  private final int id;

  private final Queue<Message> messages = new ConcurrentLinkedQueue<>();
  private final ConcurrentSkipListMap<Long, Message> byOffset = new ConcurrentSkipListMap<>();

  private volatile long nextOffset = 0;

  private final Wal wal;

  private final ReentrantLock writeLock = new ReentrantLock();

  public Partition(int id, Wal wal) throws IOException {
    this(UNKNOWN_TOPIC, id, wal);
  }

  public Partition(String topicName, int id, Wal wal) throws IOException {
    this.topicName = topicName;
    this.id = id;
    this.wal = wal;

    recover();
  }

  private void recover() throws IOException {

    var recoveredMessages = wal.read();

    for (Message message : recoveredMessages) {
      messages.offer(message);
      byOffset.put(message.offset(), message);

      nextOffset = Math.max(nextOffset, message.offset() + 1);
    }
  }

  public Message append(String key, String payload) throws IOException {

    writeLock.lock();

    try {
      long offset = nextOffset;

      Message message = Message.create(key, payload, id, offset);

      wal.append(message);

      messages.offer(message);
      byOffset.put(message.offset(), message);

      nextOffset++;

      return message;

    } finally {
      writeLock.unlock();
    }
  }

  public void applyReplicated(Message message) throws IOException {

    writeLock.lock();

    try {
      if (message.offset() < nextOffset) {
        return;
      }

      wal.append(message);

      messages.offer(message);
      byOffset.put(message.offset(), message);

      nextOffset = message.offset() + 1;

    } finally {
      writeLock.unlock();
    }
  }

  public Message poll() {
    return messages.poll();
  }

  public int getId() {
    return id;
  }

  public String getTopicName() {
    return topicName;
  }

  public long size() {
    return messages.size();
  }

  /**
   * The offset the next appended message will receive. Useful for validating a caller-supplied
   * offset and for computing consumer-group lag.
   */
  public long nextOffset() {
    return nextOffset;
  }

  /**
   * Non-destructive, offset-indexed read - unlike poll(), this never removes the message from the
   * partition. This is what backs at-least-once, offset-based consumption.
   */
  public Optional<Message> get(long offset) {
    return Optional.ofNullable(byOffset.get(offset));
  }

  /** Non-destructive read of every message from fromOffset (inclusive) onward, in order. */
  public List<Message> readFrom(long fromOffset) {
    return byOffset.tailMap(fromOffset, true).values().stream().collect(Collectors.toList());
  }
}
