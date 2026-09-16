package com.surajpanda.dmq.topic;

import com.surajpanda.dmq.partition.Partition;
import com.surajpanda.dmq.wal.Wal;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Service;

@Service
public class TopicManager {

  private static final Path DEFAULT_DATA_DIRECTORY = Path.of("data");
  private static final String METADATA_FILE_NAME = "topics.meta";

  private final Map<String, Topic> topics = new ConcurrentHashMap<>();
  private final Path dataDirectory;
  private final Path metadataFile;

  public TopicManager() throws IOException {
    this(DEFAULT_DATA_DIRECTORY);
  }

  public TopicManager(Path dataDirectory) throws IOException {
    this.dataDirectory = dataDirectory;
    this.metadataFile = dataDirectory.resolve(METADATA_FILE_NAME);

    Files.createDirectories(dataDirectory);

    recover();
  }

  private void recover() throws IOException {

    if (!Files.exists(metadataFile)) {
      return;
    }

    for (String line : Files.readAllLines(metadataFile)) {

      if (line.isBlank()) {
        continue;
      }

      String[] parts = line.split("\\|", -1);

      if (parts.length != 2) {
        throw new IOException("Corrupted topic metadata record: " + line);
      }

      String name = parts[0];
      int partitionCount = Integer.parseInt(parts[1]);

      if (topics.containsKey(name)) {
        continue;
      }

      topics.put(name, buildTopic(name, partitionCount));
    }
  }

  public Topic createTopic(String name, int partitionCount) throws IOException {

    if (partitionCount <= 0) {
      throw new IllegalArgumentException("Partition count must be greater than zero");
    }

    if (topics.containsKey(name)) {
      throw new IllegalArgumentException("Topic already exists: " + name);
    }

    Topic topic = buildTopic(name, partitionCount);

    persistMetadata(name, partitionCount);

    topics.put(name, topic);

    return topic;
  }

  public Topic getTopic(String name) {
    return topics.get(name);
  }

  private Topic buildTopic(String name, int partitionCount) throws IOException {

    var partitions = new ArrayList<Partition>();

    for (int i = 0; i < partitionCount; i++) {

      Path walPath = dataDirectory.resolve(name).resolve("partition-" + i + ".log");

      Wal wal = new Wal(walPath);

      partitions.add(new Partition(i, wal));
    }

    return new Topic(name, partitions);
  }

  private void persistMetadata(String name, int partitionCount) throws IOException {

    String record = name + "|" + partitionCount + System.lineSeparator();

    Files.writeString(metadataFile, record, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
  }
}
