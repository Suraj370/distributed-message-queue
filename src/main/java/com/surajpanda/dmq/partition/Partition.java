package com.surajpanda.dmq.partition;

import com.surajpanda.dmq.message.Message;
import com.surajpanda.dmq.wal.Wal;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class Partition {

  private final int id;
  private final Queue<Message> messages = new ConcurrentLinkedQueue<>();
  private final AtomicLong nextOffset = new AtomicLong(0);
  private final Wal wal;

  public Partition(int id, Wal wal) {
    this.id = id;
    this.wal = wal;
  }

  public Message append(String key, String payload) throws IOException {

    long offset = nextOffset.getAndIncrement();

    Message message = Message.create(key, payload, id, offset);

    wal.append(message);

    messages.offer(message);

    return message;
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
