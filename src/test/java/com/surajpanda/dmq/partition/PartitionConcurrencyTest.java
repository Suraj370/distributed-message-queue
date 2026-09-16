package com.surajpanda.dmq.partition;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.surajpanda.dmq.message.Message;
import com.surajpanda.dmq.wal.Wal;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PartitionConcurrencyTest {

  @TempDir Path tempDir;

  @Test
  void shouldAssignUniqueSequentialOffsetsConcurrently() throws Exception {

    Path walFile = tempDir.resolve("partition-0.log");

    Wal wal = new Wal(walFile);

    Partition partition = new Partition(0, wal);

    int threadCount = 100;
    int messagesPerThread = 1000;
    int expectedMessageCount = threadCount * messagesPerThread;

    ExecutorService executor = Executors.newFixedThreadPool(threadCount);

    List<Future<?>> futures = new ArrayList<>();

    for (int i = 0; i < threadCount; i++) {

      int threadId = i;

      futures.add(
          executor.submit(
              () -> {
                for (int j = 0; j < messagesPerThread; j++) {

                  partition.append("key-" + threadId, "message-" + j);
                }

                return null;
              }));
    }

    for (Future<?> future : futures) {
      future.get();
    }

    executor.shutdown();

    assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

    assertEquals(expectedMessageCount, partition.size());

    List<Message> messages = new ArrayList<>();

    Message message;

    while ((message = partition.poll()) != null) {
      messages.add(message);
    }

    assertEquals(expectedMessageCount, messages.size());

    var offsets = messages.stream().map(Message::offset).sorted().toList();

    for (int i = 0; i < expectedMessageCount; i++) {

      assertEquals(i, offsets.get(i));
    }

    assertTrue(messages.stream().allMatch(m -> m.partition() == 0));
  }
}
