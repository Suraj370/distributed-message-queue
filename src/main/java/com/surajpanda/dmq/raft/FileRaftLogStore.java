package com.surajpanda.dmq.raft;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Whole-log-replacement, file-backed RaftLogStore. Each replace() call: (1) serializes the complete
 * intended log to a sibling temporary file, (2) fully writes and fsyncs that temp file, then (3)
 * atomically renames it over the real file (ATOMIC_MOVE + REPLACE_EXISTING, falling back to a
 * plain, non-atomic move only if the filesystem genuinely does not support atomic same-directory
 * moves - see promoteTempFile()). Steps 1-2 can fail without touching the real file at all; step 3
 * is a single filesystem rename, which POSIX and NTFS both perform as one atomic metadata update -
 * so from an observer's perspective the real file is always either the old complete log or the new
 * complete log, never a partial mutation of either.
 *
 * <p>An earlier append-only design wrote one buffer per mutation via FileChannel.write()+force()
 * and treated that as "atomic enough." It was not: write() can make partial progress, and a later
 * write() or force() call can then fail, so a multi-entry mutation could still end up partially
 * durable (e.g. entry A landed, entry B did not) even though the caller only ever saw one thrown
 * exception. Routing every mutation through a temp file that is either fully promoted or not
 * promoted at all removes that failure mode entirely for the mutation itself; a crash truncating
 * the temp file mid-write is harmless - it is simply never promoted - but this does not extend to
 * claiming immunity from every conceivable storage-hardware failure.
 *
 * <p>writeTempFile() and promoteTempFile() are protected specifically so tests can subclass this
 * class and inject a failure between "some real bytes are on disk in the temp file" and "the atomic
 * rename happens" - the scenario an opaque throws-before-any-I/O test double cannot represent.
 */
public class FileRaftLogStore implements RaftLogStore {

  private final Path file;

  public FileRaftLogStore(Path file) {
    this.file = file;
  }

  @Override
  public synchronized void replace(List<LogEntry> fullLog) {
    Path tempFile = tempFilePath();
    writeTempFile(tempFile, fullLog);
    promoteTempFile(tempFile);
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

    List<LogEntry> entries = new ArrayList<>();

    for (String line : lines) {
      if (line.isBlank()) {
        continue;
      }
      // Every write to this file is a complete, atomically-promoted snapshot (see class javadoc),
      // so - unlike an append-only format - there is no normal-operation reason for a corrupted or
      // truncated line to ever appear here. Any corruption is therefore treated as a hard failure
      // rather than leniently discarded, since silently trimming it could hide real data loss
      // instead of a benign crash-mid-write artifact.
      entries.add(parseLine(line));
    }

    return List.copyOf(entries);
  }

  /**
   * Where the temp file for the next replace() lives - a sibling of the real file, so the eventual
   * rename is a same-directory move (required for ATOMIC_MOVE to be available on essentially every
   * real filesystem).
   */
  protected Path tempFilePath() {
    return file.resolveSibling(file.getFileName().toString() + ".tmp");
  }

  /**
   * Fully writes and fsyncs the serialized log to tempFile. Overridden by tests that need to land
   * some real bytes on disk and then fail before promotion - production code always writes the
   * complete content in one call.
   */
  protected void writeTempFile(Path tempFile, List<LogEntry> fullLog) {
    writeFileContent(tempFile, serialize(fullLog));
  }

  /**
   * Atomically promotes tempFile to be the real file. ATOMIC_MOVE is expected to succeed for any
   * same-directory move on a real filesystem; the AtomicMoveNotSupportedException fallback exists
   * for completeness on filesystems that genuinely lack it, and is explicitly weaker: a crash
   * during that fallback's own (non-atomic) move could leave the real file missing or truncated.
   * Normal operation on this project's supported platforms always takes the atomic path.
   */
  protected void promoteTempFile(Path tempFile) {
    try {
      Files.move(
          tempFile, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    } catch (AtomicMoveNotSupportedException atomicMoveNotSupported) {
      try {
        Files.move(tempFile, file, StandardCopyOption.REPLACE_EXISTING);
      } catch (IOException fallbackFailure) {
        throw new RaftPersistenceException(
            "Failed to replace Raft log file (atomic move unsupported and non-atomic fallback also"
                + " failed): "
                + file,
            fallbackFailure);
      }
    } catch (IOException exception) {
      throw new RaftPersistenceException(
          "Failed to atomically replace Raft log file: " + file, exception);
    }
  }

  protected String serialize(List<LogEntry> fullLog) {
    StringBuilder content = new StringBuilder();
    for (LogEntry entry : fullLog) {
      content
          .append("E|")
          .append(entry.index())
          .append('|')
          .append(entry.term())
          .append('|')
          .append(encode(entry.command()))
          .append(System.lineSeparator());
    }
    return content.toString();
  }

  protected final void writeFileContent(Path targetFile, String content) {
    try {
      Path parent = targetFile.getParent();
      if (parent != null) {
        Files.createDirectories(parent);
      }

      ByteBuffer buffer = ByteBuffer.wrap(content.getBytes(StandardCharsets.UTF_8));

      try (FileChannel channel =
          FileChannel.open(
              targetFile,
              StandardOpenOption.CREATE,
              StandardOpenOption.WRITE,
              StandardOpenOption.TRUNCATE_EXISTING)) {

        while (buffer.hasRemaining()) {
          channel.write(buffer);
        }

        channel.force(true);
      }

    } catch (IOException exception) {
      throw new RaftPersistenceException(
          "Failed to write Raft log temp file: " + targetFile, exception);
    }
  }

  private LogEntry parseLine(String line) {

    String[] parts = line.split("\\|", -1);

    if (parts.length != 4 || !"E".equals(parts[0])) {
      throw new RaftPersistenceException("Corrupted Raft log record: " + line, null);
    }

    try {
      long index = Long.parseLong(parts[1]);
      long term = Long.parseLong(parts[2]);
      return new LogEntry(term, index, decode(parts[3]));
    } catch (RuntimeException exception) {
      throw new RaftPersistenceException("Corrupted Raft log record: " + line, exception);
    }
  }

  private static String encode(String value) {
    return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
  }

  private static String decode(String value) {
    return new String(Base64.getDecoder().decode(value), StandardCharsets.UTF_8);
  }
}
