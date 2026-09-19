package com.surajpanda.dmq.raft;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

/**
 * File-backed RaftMetadataStore. Each save() writes the whole record to a temp file, fsyncs it,
 * then atomically renames it over the real file - so a crash mid-write either leaves the previous
 * durable value intact or the new one fully written, never a half-written record. This ordering is
 * what lets handleRequestVote() persist a granted vote before the RequestVoteResponse is considered
 * final, so a restart can never cause a double vote in the same term.
 */
public class FileRaftMetadataStore implements RaftMetadataStore {

  private final Path file;

  public FileRaftMetadataStore(Path file) {
    this.file = file;
  }

  @Override
  public synchronized void save(long currentTerm, String votedFor) {

    String record = currentTerm + "|" + (votedFor == null ? "" : votedFor);

    try {
      Path parent = file.getParent();
      if (parent != null) {
        Files.createDirectories(parent);
      }

      Path tempFile = file.resolveSibling(file.getFileName().toString() + ".tmp");

      Files.writeString(
          tempFile,
          record,
          StandardOpenOption.CREATE,
          StandardOpenOption.WRITE,
          StandardOpenOption.TRUNCATE_EXISTING);

      try (FileChannel channel = FileChannel.open(tempFile, StandardOpenOption.WRITE)) {
        channel.force(true);
      }

      Files.move(
          tempFile, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

    } catch (IOException exception) {
      throw new RaftPersistenceException("Failed to persist Raft metadata to " + file, exception);
    }
  }

  @Override
  public synchronized RaftMetadataSnapshot load() {

    if (!Files.exists(file)) {
      return RaftMetadataSnapshot.initial();
    }

    String content;
    try {
      content = Files.readString(file).trim();
    } catch (IOException exception) {
      throw new RaftPersistenceException("Failed to read Raft metadata from " + file, exception);
    }

    if (content.isEmpty()) {
      return RaftMetadataSnapshot.initial();
    }

    String[] parts = content.split("\\|", -1);

    if (parts.length != 2) {
      throw new RaftPersistenceException("Corrupted Raft metadata record: " + content, null);
    }

    try {
      long term = Long.parseLong(parts[0]);
      String votedFor = parts[1].isEmpty() ? null : parts[1];
      return new RaftMetadataSnapshot(term, votedFor);
    } catch (NumberFormatException exception) {
      throw new RaftPersistenceException("Corrupted Raft metadata record: " + content, exception);
    }
  }
}
