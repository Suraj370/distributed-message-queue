package com.surajpanda.dmq.wal;

import com.surajpanda.dmq.message.Message;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class Wal {

  private final Path file;

  public Wal(Path file) {
    this.file = file;
  }

  public void append(Message message) throws IOException {

    String record =
        String.format(
            "%s|%s|%s|%s|%d|%d%n",
            message.id(),
            message.key(),
            message.payload(),
            message.timestamp(),
            message.partition(),
            message.offset());

    Files.writeString(file, record, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
  }

  public List<Message> read() throws IOException {
    if (!Files.exists(file)) {
      return List.of();
    }

    List<String> lines = Files.readAllLines(file);

    List<Message> messages = new ArrayList<>();

    for (String line : lines) {

      String[] parts = line.split("\\|", -1);

      UUID id = UUID.fromString(parts[0]);
      String key = parts[1];
      String payload = parts[2];
      Instant timestamp = Instant.parse(parts[3]);
      int partition = Integer.parseInt(parts[4]);
      long offset = Long.parseLong(parts[5]);

      Message message = new Message(id, key, payload, timestamp, partition, offset);

      messages.add(message);
    }

    return messages;
  }
}
