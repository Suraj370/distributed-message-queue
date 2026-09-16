package com.surajpanda.dmq.wal;

import static org.junit.jupiter.api.Assertions.*;

import com.surajpanda.dmq.message.Message;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WalTest {

  @TempDir Path tempDir;

  @Test
  void shouldAppendMessageToWal() throws Exception {

    Path walFile = tempDir.resolve("partition-0.log");

    Wal wal = new Wal(walFile);

    Message message =
        new Message(UUID.randomUUID(), "customer-123", "order-created", Instant.now(), 0, 0);

    wal.append(message);

    assertTrue(Files.exists(walFile));

    String content = Files.readString(walFile);

    assertTrue(content.contains(message.id().toString()));
    assertTrue(content.contains(message.key()));
    assertTrue(content.contains(message.payload()));
    assertTrue(content.contains(String.valueOf(message.partition())));
    assertTrue(content.contains(String.valueOf(message.offset())));
  }

  @Test
  void shouldRecoverMessagesFromWal() throws Exception {
    Path walFile = tempDir.resolve("partition-0.log");

    Wal wal = new Wal(walFile);
    Message first =
        new Message(UUID.randomUUID(), "customer-1", "order-created", Instant.now(), 0, 0);
    Message second =
        new Message(UUID.randomUUID(), "customer-2", "order-paid", Instant.now(), 0, 1);
    Message third =
        new Message(UUID.randomUUID(), "customer-3", "order-shipped", Instant.now(), 0, 2);
    wal.append(first);
    wal.append(second);
    wal.append(third);

    Wal recoveredWal = new Wal(walFile);

    var recoveredMessages = recoveredWal.read();

    assertEquals(3, recoveredMessages.size());

    assertEquals(first.id(), recoveredMessages.get(0).id());
    assertEquals(first.key(), recoveredMessages.get(0).key());
    assertEquals(first.payload(), recoveredMessages.get(0).payload());

    assertEquals(second.id(), recoveredMessages.get(1).id());
    assertEquals(second.key(), recoveredMessages.get(1).key());
    assertEquals(second.payload(), recoveredMessages.get(1).payload());

    assertEquals(third.id(), recoveredMessages.get(2).id());
    assertEquals(third.key(), recoveredMessages.get(2).key());
    assertEquals(third.payload(), recoveredMessages.get(2).payload());
  }

  @Test
  void shouldIgnoreIncompleteFinalRecordDuringRecovery() throws Exception {

    Path walFile = tempDir.resolve("partition-0.log");

    Wal wal = new Wal(walFile);

    Message first =
        new Message(UUID.randomUUID(), "customer-1", "order-created", Instant.now(), 0, 0);

    Message second =
        new Message(UUID.randomUUID(), "customer-2", "order-paid", Instant.now(), 0, 1);

    wal.append(first);
    wal.append(second);

    // Simulate a crash during the next WAL write.
    Files.writeString(walFile, "this-is-an-incomplete-record", StandardOpenOption.APPEND);

    Wal recoveredWal = new Wal(walFile);

    var recoveredMessages = recoveredWal.read();

    assertEquals(2, recoveredMessages.size());

    assertEquals(first.id(), recoveredMessages.get(0).id());
    assertEquals(second.id(), recoveredMessages.get(1).id());
  }

  @Test
  void shouldCreateParentDirectoriesWhenAppending() throws Exception {

    Path walFile = tempDir.resolve("nested/dir/partition-0.log");

    Wal wal = new Wal(walFile);

    Message message =
        new Message(UUID.randomUUID(), "customer-1", "order-created", Instant.now(), 0, 0);

    wal.append(message);

    assertTrue(Files.exists(walFile));

    var recoveredMessages = wal.read();

    assertEquals(1, recoveredMessages.size());
    assertEquals(message.id(), recoveredMessages.get(0).id());
  }

  @Test
  void shouldFailRecoveryWhenMiddleRecordIsCorrupted() throws Exception {

    Path walFile = tempDir.resolve("partition-0.log");

    Wal wal = new Wal(walFile);

    Message first =
        new Message(UUID.randomUUID(), "customer-1", "order-created", Instant.now(), 0, 0);

    Message second =
        new Message(UUID.randomUUID(), "customer-2", "order-paid", Instant.now(), 0, 1);

    wal.append(first);

    Files.writeString(walFile, "corrupted-middle-record\n", StandardOpenOption.APPEND);

    wal.append(second);

    Wal recoveredWal = new Wal(walFile);

    assertThrows(IOException.class, recoveredWal::read);
  }
}
