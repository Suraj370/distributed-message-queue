package com.surajpanda.dmq.partition;

import com.surajpanda.dmq.message.Message;
import com.surajpanda.dmq.wal.Wal;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;

public class Partition {

  private final int id;

  private final Queue<Message> messages = new ConcurrentLinkedQueue<>();

  private long nextOffset = 0;

  private final Wal wal;

  private final ReentrantLock writeLock = new ReentrantLock();

  public Partition(int id, Wal wal) throws IOException {
    this.id = id;
    this.wal = wal;

    recover();
  }

  private void recover() throws IOException {

    var recoveredMessages = wal.read();

    for (Message message : recoveredMessages) {
      messages.offer(message);

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

  public long size() {
    return messages.size();
  }
}
