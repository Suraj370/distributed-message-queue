package com.surajpanda.dmq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.surajpanda.dmq.consumer.Consumer;
import com.surajpanda.dmq.consumer.ConsumerGroup;
import com.surajpanda.dmq.consumer.FileOffsetStore;
import com.surajpanda.dmq.message.Message;
import com.surajpanda.dmq.partition.Partition;
import com.surajpanda.dmq.queue.NotLeaderException;
import com.surajpanda.dmq.queue.QuorumUnavailableException;
import com.surajpanda.dmq.raft.RaftState;
import com.surajpanda.dmq.support.RealBrokerCluster;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Real multi-broker integration tests: each broker in the cluster is an actual Spring Boot
 * application context on its own local port, talking to the others over the genuine HTTP Raft
 * transport (see RealBrokerCluster) - unlike SimulatedCluster/DistributedFailureTest, which
 * exercise the same Raft algorithms in-process with no networking. These tests prove the transport
 * wiring in RaftConfiguration actually works end to end: real elections, real replication, real
 * majority commit, and real failure/recovery over sockets.
 *
 * <p>Deliberately no Testcontainers yet (see the milestone's own scope) - these are plain JVM
 * processes-in-threads, one Spring context per simulated broker.
 */
@Timeout(120)
class RealMultiBrokerClusterTest {

  private static final String TOPIC = "orders";

  @TempDir Path tempDir;

  @Test
  void threeBrokerClusterFormsAndElectsExactlyOneLeader() {
    try (RealBrokerCluster cluster = new RealBrokerCluster(3, tempDir)) {
      int leader = cluster.awaitLeader(Duration.ofSeconds(15));

      int leaderCount = 0;
      for (int i = 0; i < cluster.size(); i++) {
        if (cluster.raftNode(i).getState() == RaftState.LEADER) {
          leaderCount++;
        }
      }

      assertEquals(1, leaderCount);
      assertTrue(leader >= 0 && leader < cluster.size());
    }
  }

  @Test
  void publishThroughLeaderReplicatesAndAppliesToEveryBroker() throws Exception {
    try (RealBrokerCluster cluster = new RealBrokerCluster(3, tempDir)) {
      int leader = cluster.awaitLeader(Duration.ofSeconds(15));
      cluster.createTopicOnAllRunningBrokers(TOPIC, 1);

      Message published = publishWithRetry(cluster, leader, "key-1", "payload-1");
      assertEquals("payload-1", published.payload());

      for (int i = 0; i < cluster.size(); i++) {
        int brokerIndex = i;
        assertTrue(
            RealBrokerCluster.await(
                Duration.ofSeconds(10), () -> partition(cluster, brokerIndex).nextOffset() == 1),
            cluster.id(i) + " did not apply the committed publish");
        assertEquals("payload-1", partition(cluster, i).get(0).orElseThrow().payload());
      }
    }
  }

  @Test
  void oneBrokerDownStillAllowsCommitsBecauseMajorityOfThreeIsStillTwo() throws Exception {
    try (RealBrokerCluster cluster = new RealBrokerCluster(3, tempDir)) {
      int leader = cluster.awaitLeader(Duration.ofSeconds(15));
      cluster.createTopicOnAllRunningBrokers(TOPIC, 1);

      int down = (leader + 1) % cluster.size();
      int survivor = (leader + 2) % cluster.size();
      cluster.stop(down);

      Message published = publishWithRetry(cluster, leader, "key-1", "payload-1");
      assertEquals("payload-1", published.payload());

      assertTrue(
          RealBrokerCluster.await(
              Duration.ofSeconds(10), () -> partition(cluster, survivor).nextOffset() == 1));
    }
  }

  @Test
  void twoBrokersDownPreventsNewCommits() throws Exception {
    try (RealBrokerCluster cluster = new RealBrokerCluster(3, tempDir)) {
      int leader = cluster.awaitLeader(Duration.ofSeconds(15));
      cluster.createTopicOnAllRunningBrokers(TOPIC, 1);

      for (int i = 0; i < cluster.size(); i++) {
        if (i != leader) {
          cluster.stop(i);
        }
      }

      assertThrows(
          QuorumUnavailableException.class,
          () -> cluster.broker(leader).publish(TOPIC, "key-1", "payload-1"));
    }
  }

  @Test
  void restartedFollowerCatchesUpAutomaticallyWithoutAnyNewPublish() throws Exception {
    try (RealBrokerCluster cluster = new RealBrokerCluster(3, tempDir)) {
      int leader = cluster.awaitLeader(Duration.ofSeconds(15));
      cluster.createTopicOnAllRunningBrokers(TOPIC, 1);

      int laggingFollower = (leader + 1) % cluster.size();
      cluster.stop(laggingFollower);

      publishWithRetry(cluster, leader, "key-1", "payload-1");

      // Restarted broker recovers its own already-committed entries/messages from disk, then
      // rejoins the live cluster on the same configured port - no re-creation of the topic needed,
      // TopicManager recovers it from the data directory it already wrote before it was stopped.
      cluster.restart(laggingFollower);

      // No further publish happens - the leader's periodic scheduler tick (see
      // RaftConfiguration's replicateIfLeader hook) is what must catch this broker up on its own.
      assertTrue(
          RealBrokerCluster.await(
              Duration.ofSeconds(10), () -> partition(cluster, laggingFollower).nextOffset() == 1),
          "restarted follower did not catch up automatically without a new publish");
      assertEquals("payload-1", partition(cluster, laggingFollower).get(0).orElseThrow().payload());
    }
  }

  @Test
  void killingTheLeaderAllowsANewLeaderToBeElectedAndContinuePublishing() throws Exception {
    try (RealBrokerCluster cluster = new RealBrokerCluster(3, tempDir)) {
      int firstLeader = cluster.awaitLeader(Duration.ofSeconds(15));
      cluster.createTopicOnAllRunningBrokers(TOPIC, 1);

      publishWithRetry(cluster, firstLeader, "key-1", "payload-1");

      cluster.stop(firstLeader);

      int newLeader = awaitLeaderExcluding(cluster, firstLeader);
      assertTrue(newLeader != firstLeader);

      Message published = publishWithRetry(cluster, newLeader, "key-2", "payload-2");
      assertEquals("payload-2", published.payload());
    }
  }

  @Test
  void restartedOldLeaderRejoinsWithoutCorruptingCommittedState() throws Exception {
    try (RealBrokerCluster cluster = new RealBrokerCluster(3, tempDir)) {
      int firstLeader = cluster.awaitLeader(Duration.ofSeconds(15));
      cluster.createTopicOnAllRunningBrokers(TOPIC, 1);

      publishWithRetry(cluster, firstLeader, "key-1", "payload-1");
      cluster.stop(firstLeader);

      int newLeader = awaitLeaderExcluding(cluster, firstLeader);
      publishWithRetry(cluster, newLeader, "key-2", "payload-2");

      cluster.restart(firstLeader);

      // No further publish happens - the new leader's periodic scheduler tick must catch the
      // rejoined broker up on its own. This scenario legitimately involves two leadership
      // transitions in a row: without a PreVote extension (out of scope for this milestone - see
      // README), a restarted former leader's own election timeout can fire before it hears from
      // the new leader, bumping the term and forcing a real step-down/re-election cycle even
      // though its stale log guarantees it can never actually win. publishWithRetry already
      // tolerates that by retrying against whichever broker is leader at the time, which can
      // occasionally mean a publish's *first* attempt actually reached majority commit just as
      // this node stepped down, and the retry commits a second, equally legitimate entry - not
      // corruption, just an at-least-once retry doing what it is meant to do. So instead of a
      // brittle exact offset count, assert what actually matters: firstLeader converges to
      // *exactly* the same state as another broker that was never stopped, with no gap and no
      // divergence in content.
      assertTrue(
          RealBrokerCluster.await(
              Duration.ofSeconds(20),
              () ->
                  partition(cluster, firstLeader).nextOffset() >= 2
                      && partition(cluster, firstLeader).nextOffset()
                          == partition(cluster, newLeader).nextOffset()),
          "rejoined broker did not converge with the rest of the cluster");

      long convergedOffset = partition(cluster, firstLeader).nextOffset();
      for (long offset = 0; offset < convergedOffset; offset++) {
        assertEquals(
            partition(cluster, newLeader).get(offset).orElseThrow().payload(),
            partition(cluster, firstLeader).get(offset).orElseThrow().payload(),
            "content diverged at offset " + offset);
      }

      // The rejoined broker must not have forced its way back into leadership with a stale log.
      assertEquals(RaftState.FOLLOWER, cluster.raftNode(firstLeader).getState());
    }
  }

  @Test
  void consumerOffsetsAndQueueStateSurviveABrokerRestart() throws Exception {
    try (RealBrokerCluster cluster = new RealBrokerCluster(3, tempDir)) {
      int leader = cluster.awaitLeader(Duration.ofSeconds(15));
      cluster.createTopicOnAllRunningBrokers(TOPIC, 1);

      publishWithRetry(cluster, leader, "key-1", "payload-1");
      publishWithRetry(cluster, leader, "key-2", "payload-2");

      Path offsetsDir = tempDir.resolve(cluster.id(leader)).resolve("offsets");
      Partition partition = partition(cluster, leader);
      ConsumerGroup group = new ConsumerGroup("test-group", new FileOffsetStore(offsetsDir));
      Consumer consumer = new Consumer(partition);

      Message first = consumer.fetchNext(group).orElseThrow();
      assertEquals("payload-1", first.payload());
      group.commitOffset(partition, first.offset() + 1);

      cluster.restart(leader);

      Partition restartedPartition = partition(cluster, leader);
      ConsumerGroup restartedGroup =
          new ConsumerGroup("test-group", new FileOffsetStore(offsetsDir));
      Consumer restartedConsumer = new Consumer(restartedPartition);

      assertEquals(2, restartedPartition.nextOffset());
      assertEquals(1, restartedGroup.getCommittedOffset(restartedPartition));

      Message second = restartedConsumer.fetchNext(restartedGroup).orElseThrow();
      assertEquals("payload-2", second.payload());
    }
  }

  private static Partition partition(RealBrokerCluster cluster, int index) {
    return cluster.topics(index).getTopic(TOPIC).getPartition(0);
  }

  /**
   * propose() deliberately does not retry (that is a caller's responsibility, per its own javadoc)
   * - right after a fresh election, with zero slack (e.g. 2 of 3 configured brokers alive, so every
   * survivor must ack), a single transient hiccup (a slightly slow response, a follower briefly
   * busy with its own election bookkeeping, or even a legitimate spurious re-election racing real
   * OS-scheduled processes) can fail one round or move leadership even though the cluster is
   * healthy. A real caller is expected to retry and rediscover the leader; this mirrors that
   * instead of asserting zero-hiccup timing on real wall-clock/OS-scheduled processes.
   */
  private static Message publishWithRetry(
      RealBrokerCluster cluster, int brokerIndex, String key, String payload) {
    int target = brokerIndex;
    RuntimeException lastFailure = null;
    for (int attempt = 0; attempt < 30; attempt++) {
      try {
        return cluster.broker(target).publish(TOPIC, key, payload);
      } catch (QuorumUnavailableException exception) {
        lastFailure = exception;
      } catch (NotLeaderException exception) {
        lastFailure = exception;
        for (int i = 0; i < cluster.size(); i++) {
          if (cluster.isRunning(i) && cluster.raftNode(i).getState() == RaftState.LEADER) {
            target = i;
            break;
          }
        }
      }
      try {
        Thread.sleep(50);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw lastFailure;
      }
    }
    throw lastFailure;
  }

  private static int awaitLeaderExcluding(RealBrokerCluster cluster, int excluded) {
    boolean elected =
        RealBrokerCluster.await(
            Duration.ofSeconds(15),
            () -> {
              for (int i = 0; i < cluster.size(); i++) {
                if (i != excluded
                    && cluster.isRunning(i)
                    && cluster.raftNode(i).getState() == RaftState.LEADER) {
                  return true;
                }
              }
              return false;
            });
    assertTrue(elected, "no new leader was elected excluding " + cluster.id(excluded));

    for (int i = 0; i < cluster.size(); i++) {
      if (i != excluded
          && cluster.isRunning(i)
          && cluster.raftNode(i).getState() == RaftState.LEADER) {
        return i;
      }
    }
    throw new IllegalStateException("unreachable");
  }
}
