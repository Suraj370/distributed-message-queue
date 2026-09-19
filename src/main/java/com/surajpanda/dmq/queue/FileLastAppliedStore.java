package com.surajpanda.dmq.queue;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

/**
 * File-backed LastAppliedStore using the same write-temp-then-atomic-rename pattern as
 * FileRaftMetadataStore, so a crash mid-write never leaves a half-written value.
 */
public class FileLastAppliedStore implements LastAppliedStore {

  private final Path file;

  public FileLastAppliedStore(Path file) {
    this.file = file;
  }

  @Override
  public synchronized void save(long lastAppliedIndex) {

    try {
      Path parent = file.getParent();
      if (parent != null) {
        Files.createDirectories(parent);
      }

      Path tempFile = file.resolveSibling(file.getFileName().toString() + ".tmp");

      Files.writeString(
          tempFile,
          Long.toString(lastAppliedIndex),
          StandardOpenOption.CREATE,
          StandardOpenOption.WRITE,
          StandardOpenOption.TRUNCATE_EXISTING);

      try (FileChannel channel = FileChannel.open(tempFile, StandardOpenOption.WRITE)) {
        channel.force(true);
      }

      Files.move(
          tempFile, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

    } catch (IOException exception) {
      throw new QueuePersistenceException(
          "Failed to persist last-applied index to " + file, exception);
    }
  }

  @Override
  public synchronized long load() {

    if (!Files.exists(file)) {
      return 0;
    }

    try {
      String content = Files.readString(file).trim();
      return content.isEmpty() ? 0 : Long.parseLong(content);
    } catch (IOException exception) {
      throw new QueuePersistenceException(
          "Failed to read last-applied index from " + file, exception);
    }
  }
}
