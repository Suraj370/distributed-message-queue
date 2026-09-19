package com.surajpanda.dmq.consumer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Append-only, file-backed OffsetStore: one file per consumer group (data/offsets/&lt;groupId&gt;
 * .offsets), one "topic|partitionId|offset" line per commit. Recovery replays every line in order;
 * the last line for a given (topic, partitionId) wins, so no in-place rewrite is needed to update
 * an offset - only ever appended, consistent with the project's WAL-style persistence.
 */
public class FileOffsetStore implements OffsetStore {

  private final Path directory;

  public FileOffsetStore(Path directory) {
    this.directory = directory;
  }

  @Override
  public synchronized void save(String groupId, PartitionKey partitionKey, long offset) {

    String record = partitionKey.topic() + "|" + partitionKey.partitionId() + "|" + offset;

    try {
      Files.createDirectories(directory);

      ByteBuffer buffer =
          ByteBuffer.wrap((record + System.lineSeparator()).getBytes(StandardCharsets.UTF_8));

      try (FileChannel channel =
          FileChannel.open(
              fileFor(groupId),
              StandardOpenOption.CREATE,
              StandardOpenOption.WRITE,
              StandardOpenOption.APPEND)) {

        while (buffer.hasRemaining()) {
          channel.write(buffer);
        }

        channel.force(true);
      }

    } catch (IOException exception) {
      throw new OffsetPersistenceException(
          "Failed to persist offset for group " + groupId, exception);
    }
  }

  @Override
  public synchronized Map<PartitionKey, Long> loadAll(String groupId) {

    Path file = fileFor(groupId);

    if (!Files.exists(file)) {
      return Map.of();
    }

    List<String> lines;
    try {
      lines = Files.readAllLines(file);
    } catch (IOException exception) {
      throw new OffsetPersistenceException(
          "Failed to read offsets for group " + groupId, exception);
    }

    Map<PartitionKey, Long> offsets = new HashMap<>();

    for (int i = 0; i < lines.size(); i++) {

      String line = lines.get(i);

      if (line.isBlank()) {
        continue;
      }

      try {
        String[] parts = line.split("\\|", -1);
        if (parts.length != 3) {
          throw new IllegalArgumentException("Invalid offset record: " + line);
        }
        offsets.put(
            new PartitionKey(parts[0], Integer.parseInt(parts[1])), Long.parseLong(parts[2]));
      } catch (RuntimeException exception) {

        if (i == lines.size() - 1) {
          break;
        }

        throw new OffsetPersistenceException(
            "Corrupted offset record at line " + i + " for group " + groupId, exception);
      }
    }

    return offsets;
  }

  private Path fileFor(String groupId) {
    return directory.resolve(groupId + ".offsets");
  }
}
