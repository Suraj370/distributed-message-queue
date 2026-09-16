package com.surajpanda.dmq.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.surajpanda.dmq.message.Message;
import com.surajpanda.dmq.partition.Partition;
import com.surajpanda.dmq.wal.Wal;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PartitionReplicatorTest {

  @TempDir Path tempDir;

  @Test
  void shouldReplicateAnAppendedMessageToAReplica() throws Exception {

    Partition primary = new Partition(0, new Wal(tempDir.resolve("primary.log")));
    Partition replica = new Partition(0, new Wal(tempDir.resolve("replica.log")));

    PartitionReplicator replicator =
        new PartitionReplicator(
            List.of(new ReplicaTarget("broker-2", new InProcessReplicaConnection(replica))));

    Message message = primary.append("key-1", "message-1");

    ReplicationReport report = replicator.replicate(message);

    assertTrue(report.fullySucceeded());
    assertEquals("message-1", replica.poll().payload());
  }

  @Test
  void shouldReplicateToMultipleReplicasPreservingFields() throws Exception {

    Partition primary = new Partition(0, new Wal(tempDir.resolve("primary.log")));
    Partition replicaA = new Partition(0, new Wal(tempDir.resolve("replica-a.log")));
    Partition replicaB = new Partition(0, new Wal(tempDir.resolve("replica-b.log")));

    PartitionReplicator replicator =
        new PartitionReplicator(
            List.of(
                new ReplicaTarget("broker-2", new InProcessReplicaConnection(replicaA)),
                new ReplicaTarget("broker-3", new InProcessReplicaConnection(replicaB))));

    Message original = primary.append("key-1", "message-1");

    replicator.replicate(original);

    Message onA = replicaA.poll();
    Message onB = replicaB.poll();

    assertEquals(original.id(), onA.id());
    assertEquals(original.offset(), onA.offset());
    assertEquals(original.timestamp(), onA.timestamp());

    assertEquals(original.id(), onB.id());
    assertEquals(original.offset(), onB.offset());
    assertEquals(original.timestamp(), onB.timestamp());
  }

  @Test
  void shouldPreserveOrderingAcrossMultipleReplicatedMessages() throws Exception {

    Partition primary = new Partition(0, new Wal(tempDir.resolve("primary.log")));
    Partition replica = new Partition(0, new Wal(tempDir.resolve("replica.log")));

    PartitionReplicator replicator =
        new PartitionReplicator(
            List.of(new ReplicaTarget("broker-2", new InProcessReplicaConnection(replica))));

    replicator.replicate(primary.append("key-1", "message-1"));
    replicator.replicate(primary.append("key-2", "message-2"));
    replicator.replicate(primary.append("key-3", "message-3"));

    assertEquals("message-1", replica.poll().payload());
    assertEquals("message-2", replica.poll().payload());
    assertEquals("message-3", replica.poll().payload());
  }
}
