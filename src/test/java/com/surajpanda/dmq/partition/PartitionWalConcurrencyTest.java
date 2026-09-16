package com.surajpanda.dmq.partition;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.surajpanda.dmq.wal.Wal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PartitionWalConcurrencyTest {

  @TempDir Path tempDir;

  @Test
  void shouldWriteWalRecordsInOffsetOrderConcurrently() throws Exception {

    Path walFile = tempDir.resolve("partition-0.log");

    Wal wal = new Wal(walFile);
    Partition partition = new Partition(0, wal);

    int threadCount = 20;
    int messagesPerThread = 100;
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

    List<String> lines = Files.readAllLines(walFile);

    assertEquals(expectedMessageCount, lines.size());

    for (int i = 0; i < lines.size(); i++) {

      String[] parts = lines.get(i).split("\\|", -1);

      long offset = Long.parseLong(parts[5]);

      assertEquals(i, offset);
    }
  }
}
