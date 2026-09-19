package com.surajpanda.dmq.raft;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Append-only, file-backed RaftLogStore, in the same spirit as the queue's Wal: every mutation is
 * one more line appended and fsynced, never an in-place rewrite. An entry is recorded as
 * "E|index|term|base64(command)"; a conflicting-suffix truncation is recorded as its own "T|index"
 * tombstone line rather than rewriting history. Recovery replays every line in order, applying each
 * truncation as it is encountered, so the final reconstructed log reflects exactly what a live
 * RaftLog would have converged to - truncated entries are simply absent, not merely marked.
 *
 * <p>A corrupted or incomplete final line (e.g. a crash mid-write) is discarded during recovery,
 * consistent with the queue Wal's own recovery policy; a corrupted line anywhere else is a hard
 * failure, since silently dropping the middle of the log would be a durability violation.
 */
public class FileRaftLogStore implements RaftLogStore {

  private final Path file;

  public FileRaftLogStore(Path file) {
    this.file = file;
  }

  @Override
  public synchronized void append(LogEntry entry) {
    writeLine("E|" + entry.index() + "|" + entry.term() + "|" + encode(entry.command()));
  }

  @Override
  public synchronized void truncateFrom(long index) {
    writeLine("T|" + index);
  }

  @Override
  public synchronized List<LogEntry> loadAll() {

    if (!Files.exists(file)) {
      return List.of();
    }

    List<String> lines;
    try {
      lines = Files.readAllLines(file);
    } catch (IOException exception) {
      throw new RaftPersistenceException("Failed to read Raft log store: " + file, exception);
    }

    NavigableMap<Long, LogEntry> entries = new TreeMap<>();

    for (int i = 0; i < lines.size(); i++) {

      String line = lines.get(i);

      if (line.isBlank()) {
        continue;
      }

      try {
        applyLine(entries, line);
      } catch (RuntimeException exception) {

        if (i == lines.size() - 1) {
          break;
        }

        throw new RaftPersistenceException("Corrupted Raft log record at line " + i, exception);
      }
    }

    return List.copyOf(entries.values());
  }

  private void applyLine(NavigableMap<Long, LogEntry> entries, String line) {

    String[] parts = line.split("\\|", -1);

    switch (parts[0]) {
      case "E" -> {
        if (parts.length != 4) {
          throw new IllegalArgumentException("Invalid Raft log entry record: " + line);
        }
        long index = Long.parseLong(parts[1]);
        long term = Long.parseLong(parts[2]);
        entries.put(index, new LogEntry(term, index, decode(parts[3])));
      }
      case "T" -> {
        if (parts.length != 2) {
          throw new IllegalArgumentException("Invalid Raft log truncation record: " + line);
        }
        entries.tailMap(Long.parseLong(parts[1]), true).clear();
      }
      default -> throw new IllegalArgumentException("Unknown Raft log record type: " + line);
    }
  }

  private void writeLine(String record) {

    try {
      Path parent = file.getParent();
      if (parent != null) {
        Files.createDirectories(parent);
      }

      ByteBuffer buffer =
          ByteBuffer.wrap((record + System.lineSeparator()).getBytes(StandardCharsets.UTF_8));

      try (FileChannel channel =
          FileChannel.open(
              file,
              StandardOpenOption.CREATE,
              StandardOpenOption.WRITE,
              StandardOpenOption.APPEND)) {

        while (buffer.hasRemaining()) {
          channel.write(buffer);
        }

        channel.force(true);
      }

    } catch (IOException exception) {
      throw new RaftPersistenceException("Failed to persist Raft log record: " + record, exception);
    }
  }

  private static String encode(String value) {
    return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
  }

  private static String decode(String value) {
    return new String(Base64.getDecoder().decode(value), StandardCharsets.UTF_8);
  }
}
