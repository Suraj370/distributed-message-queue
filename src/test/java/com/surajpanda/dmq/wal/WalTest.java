package com.surajpanda.dmq.wal;

import com.surajpanda.dmq.message.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class WalTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldAppendMessageToWal() throws Exception {

        Path walFile = tempDir.resolve("partition-0.log");

        Wal wal = new Wal(walFile);

        Message message = new Message(
                UUID.randomUUID(),
                "customer-123",
                "order-created",
                Instant.now(),
                0,
                0
        );

        wal.append(message);

        assertTrue(Files.exists(walFile));

        String content = Files.readString(walFile);

        assertTrue(content.contains(message.id().toString()));
        assertTrue(content.contains(message.key()));
        assertTrue(content.contains(message.payload()));
        assertTrue(content.contains(String.valueOf(message.partition())));
        assertTrue(content.contains(String.valueOf(message.offset())));
    }
}