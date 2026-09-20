package com.surajpanda.dmq.raft.support;

import com.surajpanda.dmq.raft.FileRaftLogStore;
import com.surajpanda.dmq.raft.LogEntry;
import com.surajpanda.dmq.raft.RaftPersistenceException;
import java.nio.file.Path;
import java.util.List;

/**
 * A real FileRaftLogStore that can be armed to fail mid-replace() in one of two ways that a simple
 * throws-before-any-I/O wrapper (FailingRaftLogStore) cannot represent:
 *
 * <ul>
 *   <li>failDuringNextTempWrite(): writes a genuinely truncated prefix of the intended content to
 *       the temp file - real bytes land on disk - then throws, standing in for a
 *       FileChannel.write() that made partial progress before a later write()/force() call would
 *       have failed.
 *   <li>failBeforeNextReplacement(): writes the complete, valid temp file (fully written and
 *       forced, exactly as a successful run would), then throws before the atomic rename that would
 *       promote it - standing in for the promotion step itself failing (e.g. the rename call
 *       throwing) after the temp file was already durably complete.
 * </ul>
 *
 * In both cases the real target file is never touched, exactly as production code's promoteTempFile
 * would leave it - this is what lets a test prove the original durable file is still fully
 * recoverable after either kind of failure.
 */
public class PartiallyFailingRaftLogStore extends FileRaftLogStore {

  private enum Mode {
    NONE,
    DURING_TEMP_WRITE,
    BEFORE_REPLACEMENT
  }

  private volatile Mode mode = Mode.NONE;

  public PartiallyFailingRaftLogStore(Path file) {
    super(file);
  }

  public void failDuringNextTempWrite() {
    mode = Mode.DURING_TEMP_WRITE;
  }

  public void failBeforeNextReplacement() {
    mode = Mode.BEFORE_REPLACEMENT;
  }

  @Override
  protected void writeTempFile(Path tempFile, List<LogEntry> fullLog) {

    Mode current = mode;
    mode = Mode.NONE;

    if (current == Mode.DURING_TEMP_WRITE) {
      String fullContent = serialize(fullLog);
      String truncated = fullContent.substring(0, fullContent.length() / 2);
      writeFileContent(tempFile, truncated); // real, genuinely partial bytes on disk
      throw new RaftPersistenceException("Simulated failure mid temp-file write", null);
    }

    super.writeTempFile(tempFile, fullLog); // complete, valid, fully-forced temp file

    if (current == Mode.BEFORE_REPLACEMENT) {
      throw new RaftPersistenceException(
          "Simulated failure after temp file write, before atomic replacement", null);
    }
  }
}
