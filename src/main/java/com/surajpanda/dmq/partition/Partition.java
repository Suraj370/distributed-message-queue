package com.surajpanda.dmq.partition;

import com.surajpanda.dmq.message.Message;
import com.surajpanda.dmq.wal.Wal;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
  private final Set<Long> appliedRaftLogIndexes = ConcurrentHashMap.newKeySet();

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

      if (message.raftLogIndex() != null) {
        appliedRaftLogIndexes.add(message.raftLogIndex());
      }
    }
  }

  public Message append(String key, String payload) throws IOException {
    return append(key, payload, null);
  }

  /**
   * Appends a message tagged with the Raft log index whose committed application produced it. The
   * tag is durably persisted with the message (see Wal), so hasAppliedRaftIndex() keeps working
   * correctly across a restart - this is what lets RaftQueueStateMachine tell "already durably
   * applied" apart from "never applied" even if it crashes before recording its own last-applied
   * index.
   */
  public Message append(String key, String payload, Long raftLogIndex) throws IOException {

    writeLock.lock();

    try {
      long offset = nextOffset;

      Message message = Message.create(key, payload, id, offset, raftLogIndex);

      wal.append(message);

      messages.offer(message);
      byOffset.put(message.offset(), message);

      if (raftLogIndex != null) {
        appliedRaftLogIndexes.add(raftLogIndex);
      }

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

      if (message.raftLogIndex() != null) {
        appliedRaftLogIndexes.add(message.raftLogIndex());
      }

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

  /**
   * Whether a message tagged with this Raft log index has already been durably appended to this
   * partition - either earlier in this run or recovered from the WAL on startup. Lets
   * RaftQueueStateMachine detect a committed command it already applied in a previous run even if
   * it crashed before persisting its own last-applied marker, without relying on that marker alone.
   */
  public boolean hasAppliedRaftIndex(long raftLogIndex) {
    return appliedRaftLogIndexes.contains(raftLogIndex);
  }
}
