package com.surajpanda.dmq.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.surajpanda.dmq.message.Message;
import com.surajpanda.dmq.partition.Partition;
import com.surajpanda.dmq.wal.Wal;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ReplicationFailureTest {

  @TempDir Path tempDir;

  private static final class UnavailableReplicaConnection implements ReplicaConnection {

    @Override
    public ReplicationOutcome send(Message message) {
      return ReplicationOutcome.failed(new IOException("simulated replica unavailable"));
    }
  }

  @Test
  void shouldReportFailureWhenAReplicaIsUnavailableWithoutBlockingOtherReplicas() throws Exception {

    Partition primary = new Partition(0, new Wal(tempDir.resolve("primary.log")));
    Partition healthyReplica = new Partition(0, new Wal(tempDir.resolve("replica-healthy.log")));

    PartitionReplicator replicator =
        new PartitionReplicator(
            List.of(
                new ReplicaTarget("broker-2", new UnavailableReplicaConnection()),
                new ReplicaTarget("broker-3", new InProcessReplicaConnection(healthyReplica))));

    Message message = primary.append("key-1", "message-1");

    ReplicationReport report = replicator.replicate(message);

    assertFalse(report.fullySucceeded());
    assertEquals(List.of("broker-2"), report.failedReplicaBrokerIds());
    assertEquals("message-1", healthyReplica.poll().payload());
  }

  @Test
  void shouldNotCorruptPrimaryWhenAReplicaFails() throws Exception {

    Partition primary = new Partition(0, new Wal(tempDir.resolve("primary.log")));

    PartitionReplicator replicator =
        new PartitionReplicator(
            List.of(new ReplicaTarget("broker-2", new UnavailableReplicaConnection())));

    Message message = primary.append("key-1", "message-1");

    replicator.replicate(message);

    assertEquals(1, primary.size());
    assertEquals("message-1", primary.poll().payload());

    Message next = primary.append("key-2", "message-2");
    assertEquals(1, next.offset());
  }
}
