package com.surajpanda.dmq.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.surajpanda.dmq.message.Message;
import com.surajpanda.dmq.partition.Partition;
import com.surajpanda.dmq.wal.Wal;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ReplicaRecoveryTest {

  @TempDir Path tempDir;

  @Test
  void shouldRecoverReplicatedMessagesFromWalAfterReplicaRestart() throws Exception {

    Path replicaWalFile = tempDir.resolve("replica-0.log");

    Partition primary = new Partition(0, new Wal(tempDir.resolve("primary-0.log")));
    Partition replica = new Partition(0, new Wal(replicaWalFile));

    Message first = primary.append("key-1", "message-1");
    Message second = primary.append("key-2", "message-2");

    replica.applyReplicated(first);
    replica.applyReplicated(second);

    Partition recoveredReplica = new Partition(0, new Wal(replicaWalFile));

    assertEquals(2, recoveredReplica.size());
    assertEquals("message-1", recoveredReplica.poll().payload());
    assertEquals("message-2", recoveredReplica.poll().payload());

    Message third = primary.append("key-3", "message-3");
    recoveredReplica.applyReplicated(third);

    assertEquals("message-3", recoveredReplica.poll().payload());
  }

  @Test
  void shouldIsolateReplicaDataAcrossDifferentDataDirectories() throws Exception {

    Partition primary = new Partition(0, new Wal(tempDir.resolve("primary-0.log")));

    Partition broker2Replica =
        new Partition(0, new Wal(tempDir.resolve("broker-2").resolve("partition-0.log")));
    Partition broker3Replica =
        new Partition(0, new Wal(tempDir.resolve("broker-3").resolve("partition-0.log")));

    Message message = primary.append("key-1", "message-1");

    broker2Replica.applyReplicated(message);

    assertEquals("message-1", broker2Replica.poll().payload());
    assertNull(broker3Replica.poll());
  }
}
