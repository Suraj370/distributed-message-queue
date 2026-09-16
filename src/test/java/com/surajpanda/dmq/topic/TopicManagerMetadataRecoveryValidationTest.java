package com.surajpanda.dmq.topic;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TopicManagerMetadataRecoveryValidationTest {

  @TempDir Path tempDir;

  @Test
  void shouldFailRecoveryWhenPartitionCountIsNotAnInteger() throws Exception {

    Files.createDirectories(tempDir);
    Files.writeString(
        tempDir.resolve("topics.meta"),
        "orders|not-a-number" + System.lineSeparator(),
        StandardOpenOption.CREATE,
        StandardOpenOption.APPEND);

    assertThrows(IOException.class, () -> new TopicManager(tempDir));
  }

  @Test
  void shouldFailRecoveryWhenPartitionCountIsNotPositive() throws Exception {

    Files.createDirectories(tempDir);
    Files.writeString(
        tempDir.resolve("topics.meta"),
        "orders|0" + System.lineSeparator(),
        StandardOpenOption.CREATE,
        StandardOpenOption.APPEND);

    assertThrows(IOException.class, () -> new TopicManager(tempDir));
  }
}
