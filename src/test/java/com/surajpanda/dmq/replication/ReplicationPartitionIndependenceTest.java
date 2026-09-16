package com.surajpanda.dmq.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.surajpanda.dmq.message.Message;
import com.surajpanda.dmq.partition.Partition;
import com.surajpanda.dmq.wal.Wal;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ReplicationPartitionIndependenceTest {

  @TempDir Path tempDir;

  @Test
  void shouldKeepReplicationOfDifferentPartitionsIndependent() throws Exception {

    Partition primary0 = new Partition(0, new Wal(tempDir.resolve("primary-0.log")));
    Partition primary1 = new Partition(1, new Wal(tempDir.resolve("primary-1.log")));

    Partition replica0 = new Partition(0, new Wal(tempDir.resolve("replica-0.log")));
    Partition replica1 = new Partition(1, new Wal(tempDir.resolve("replica-1.log")));

    PartitionReplicator replicator0 =
        new PartitionReplicator(
            List.of(new ReplicaTarget("broker-2", new InProcessReplicaConnection(replica0))));

    PartitionReplicator replicator1 =
        new PartitionReplicator(
            List.of(new ReplicaTarget("broker-2", new InProcessReplicaConnection(replica1))));

    Message message0 = primary0.append("key-1", "partition-0-message");

    replicator0.replicate(message0);

    assertEquals("partition-0-message", replica0.poll().payload());
    assertNull(replica1.poll());
    assertEquals(0, primary1.size());

    Message message1 = primary1.append("key-2", "partition-1-message");
    assertEquals(0, message1.offset());

    replicator1.replicate(message1);

    assertEquals("partition-1-message", replica1.poll().payload());
  }
}
