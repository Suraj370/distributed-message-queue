package com.surajpanda.dmq.partition;

import com.surajpanda.dmq.message.Message;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class PartitionConcurrencyTest {

    @Test
    void shouldAssignUniqueSequentialOffsetsConcurrently()
            throws Exception {

        Partition partition = new Partition(0);

        int threadCount = 100;
        int messagesPerThread = 1000;
        int expectedMessageCount = threadCount * messagesPerThread;

        ExecutorService executor =
                Executors.newFixedThreadPool(threadCount);

        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {

            int threadId = i;

            futures.add(executor.submit(() -> {

                for (int j = 0; j < messagesPerThread; j++) {

                    partition.append(
                            "key-" + threadId,
                            "message-" + j
                    );
                }
            }));
        }

        for (Future<?> future : futures) {
            future.get();
        }

        executor.shutdown();

        assertTrue(
                executor.awaitTermination(
                        5,
                        TimeUnit.SECONDS
                )
        );

        assertEquals(
                expectedMessageCount,
                partition.size()
        );

        List<Message> messages = new ArrayList<>();

        Message message;

        while ((message = partition.poll()) != null) {
            messages.add(message);
        }

        assertEquals(
                expectedMessageCount,
                messages.size()
        );

        var offsets = messages.stream()
                .map(Message::offset)
                .sorted()
                .toList();

        for (int i = 0; i < expectedMessageCount; i++) {

            assertEquals(
                    i,
                    offsets.get(i)
            );
        }

        assertTrue(
                messages.stream()
                        .allMatch(m -> m.partition() == 0)
        );
    }
}