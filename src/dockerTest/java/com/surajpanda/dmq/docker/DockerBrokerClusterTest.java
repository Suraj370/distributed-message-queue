package com.surajpanda.dmq.docker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.web.client.RestClientException;

/**
 * Real Testcontainers integration tests: the actual broker Docker image, three real containers on a
 * real Docker network, talking to each other over real broker-to-broker Raft HTTP - and to the test
 * itself only over the real client-facing HTTP API (StatusController, MessageController,
 * ConsumerController). No internal Java object is ever touched from these tests.
 *
 * <p>Deliberately does not test persistence across a fresh container/volume - see the README's
 * Testcontainers section for why a stop+start of the *same* container (used throughout this class
 * for "restart" scenarios) already exercises the real recovery-from-disk path, and why a separate
 * named-volume test was judged unnecessary complexity for this milestone.
 */
@Timeout(300)
@ExtendWith(ContainerLogsOnFailureExtension.class)
class DockerBrokerClusterTest {

  private static final String TOPIC = "orders";

  // Deliberately no @AfterEach clearing the active cluster: TestWatcher.testFailed() runs AFTER
  // every @AfterEach callback (it observes the final outcome), so clearing here would always wipe
  // the reference before diagnostics ever got a chance to read it. Each test overwrites it at
  // start instead - a stale reference from a previous (already-closed) cluster is harmless since
  // logsFor() reads from an in-memory buffer that outlives the container.

  // PART 7: cluster formation, membership, single leader, followers recognize it.
  @Test
  void threeBrokerClusterFormsWithConfiguredMembershipAndElectsExactlyOneLeader() {
    try (DockerBrokerCluster cluster = new DockerBrokerCluster(3)) {
      ContainerLogsOnFailureExtension.setActive(cluster);

      int leader = cluster.awaitLeader(Duration.ofSeconds(60));

      int leaderCount = 0;
      for (int i = 0; i < cluster.size(); i++) {
        StatusResponse status = cluster.status(i).orElseThrow();
        assertEquals(3, status.configuredClusterSize(), cluster.id(i) + " has wrong cluster size");
        if ("LEADER".equals(status.raftState())) {
          leaderCount++;
        } else {
          assertEquals("FOLLOWER", status.raftState(), cluster.id(i) + " in unexpected state");
        }
      }

      assertEquals(1, leaderCount);
      assertTrue(leader >= 0 && leader < cluster.size());
    }
  }

  // PART 8: real publish -> replication -> majority commit -> queue state, over HTTP only.
  @Test
  void publishThroughRealApiReplicatesAndReachesQueueStateOnEveryBroker() {
    try (DockerBrokerCluster cluster = new DockerBrokerCluster(3)) {
      ContainerLogsOnFailureExtension.setActive(cluster);

      int leader = cluster.awaitLeader(Duration.ofSeconds(60));
      cluster.createTopicOnAllRunningBrokers(TOPIC, 1);

      MessageResponse published = publishWithRetry(cluster, leader, "key-1", "payload-1");
      assertEquals("payload-1", published.payload());

      for (int i = 0; i < cluster.size(); i++) {
        int brokerIndex = i;
        assertTrue(
            DockerBrokerCluster.await(
                Duration.ofSeconds(30), () -> cluster.read(brokerIndex, TOPIC, 0, 0).isPresent()),
            cluster.id(i) + " never showed the committed message in its queue state");
        assertEquals("payload-1", cluster.read(i, TOPIC, 0, 0).orElseThrow().payload());
      }
    }
  }

  // PART 9: leader failure -> new leader elected -> old committed state preserved -> continues.
  @Test
  void leaderFailureElectsANewLeaderAndPreservesCommittedState() {
    try (DockerBrokerCluster cluster = new DockerBrokerCluster(3)) {
      ContainerLogsOnFailureExtension.setActive(cluster);

      int firstLeader = cluster.awaitLeader(Duration.ofSeconds(60));
      cluster.createTopicOnAllRunningBrokers(TOPIC, 1);
      publishWithRetry(cluster, firstLeader, "key-1", "payload-1");

      cluster.stop(firstLeader);
      assertTrue(cluster.status(firstLeader).isEmpty());

      int newLeader = cluster.awaitLeaderExcluding(firstLeader, Duration.ofSeconds(60));
      assertNotEquals(firstLeader, newLeader);

      assertTrue(
          cluster.read(newLeader, TOPIC, 0, 0).isPresent(), "previously committed message lost");
      assertEquals("payload-1", cluster.read(newLeader, TOPIC, 0, 0).orElseThrow().payload());

      MessageResponse second = publishWithRetry(cluster, newLeader, "key-2", "payload-2");
      assertEquals("payload-2", second.payload());

      int survivingFollower = 3 - firstLeader - newLeader; // the third index, 0+1+2=3
      assertTrue(
          DockerBrokerCluster.await(
              Duration.ofSeconds(30),
              () -> cluster.read(survivingFollower, TOPIC, 0, 1).isPresent()),
          "surviving follower did not catch up on the new commit");
    }
  }

  // PART 10: two failures -> no quorum -> new writes cannot commit; catches the peers.size()+1 bug.
  @Test
  void quorumLossPreventsNewCommitsWithoutMisreadingReachablePeersAsTheWholeCluster() {
    try (DockerBrokerCluster cluster = new DockerBrokerCluster(3)) {
      ContainerLogsOnFailureExtension.setActive(cluster);

      int leader = cluster.awaitLeader(Duration.ofSeconds(60));
      cluster.createTopicOnAllRunningBrokers(TOPIC, 1);

      for (int i = 0; i < cluster.size(); i++) {
        if (i != leader) {
          cluster.stop(i);
        }
      }

      // The lone survivor must still report the configured cluster size as 3 (never silently
      // shrink to "1" just because it can no longer reach the other two) and must refuse to
      // commit a new write without a real majority.
      StatusResponse aloneStatus = cluster.status(leader).orElseThrow();
      assertEquals(3, aloneStatus.configuredClusterSize());

      assertThrows(
          QuorumUnavailableHttpException.class,
          () -> cluster.publish(leader, TOPIC, "key-1", "payload-1"));
    }
  }

  // PART 11: follower restart catches up automatically - no new publish required to trigger it.
  @Test
  void restartedFollowerCatchesUpAutomaticallyWithoutAnyNewPublish() {
    try (DockerBrokerCluster cluster = new DockerBrokerCluster(3)) {
      ContainerLogsOnFailureExtension.setActive(cluster);

      int leader = cluster.awaitLeader(Duration.ofSeconds(60));
      cluster.createTopicOnAllRunningBrokers(TOPIC, 1);

      int laggingFollower = (leader + 1) % cluster.size();
      cluster.stop(laggingFollower);

      publishWithRetry(cluster, leader, "key-1", "payload-1");
      publishWithRetry(cluster, leader, "key-2", "payload-2");

      cluster.restart(laggingFollower);

      // No further publish happens - the leader's periodic scheduler tick must catch this
      // broker up on its own (see RaftConfiguration's replicateIfLeader hook).
      assertTrue(
          DockerBrokerCluster.await(
              Duration.ofSeconds(30), () -> cluster.read(laggingFollower, TOPIC, 0, 1).isPresent()),
          "restarted follower did not catch up automatically");
      assertEquals("payload-1", cluster.read(laggingFollower, TOPIC, 0, 0).orElseThrow().payload());
      assertEquals("payload-2", cluster.read(laggingFollower, TOPIC, 0, 1).orElseThrow().payload());

      // No duplicates: exactly the two published messages, nothing beyond offset 1.
      assertTrue(cluster.read(laggingFollower, TOPIC, 0, 2).isEmpty());
    }
  }

  // PART 12: old leader restarts, rejoins as follower, catches up, never resumes leadership.
  @Test
  void restartedOldLeaderRejoinsAsFollowerAndConvergesWithoutDuplicatingState() {
    try (DockerBrokerCluster cluster = new DockerBrokerCluster(3)) {
      ContainerLogsOnFailureExtension.setActive(cluster);

      int firstLeader = cluster.awaitLeader(Duration.ofSeconds(60));
      cluster.createTopicOnAllRunningBrokers(TOPIC, 1);
      publishWithRetry(cluster, firstLeader, "key-1", "payload-1");

      cluster.stop(firstLeader);
      int newLeader = cluster.awaitLeaderExcluding(firstLeader, Duration.ofSeconds(60));
      publishWithRetry(cluster, newLeader, "key-2", "payload-2");

      cluster.restart(firstLeader);

      // No further publish happens - the new leader's periodic scheduler tick must catch the
      // rejoined broker up on its own. This scenario legitimately involves two leadership
      // transitions in a row: without a PreVote extension (out of scope for this milestone - see
      // README), a restarted former leader's own election timeout can fire before it hears from
      // the new leader, forcing a real (unwinnable, since its log is behind) election cycle.
      // publishWithRetry tolerates that by retrying, which can occasionally mean a publish's
      // first attempt actually reached majority commit right as this node stepped down, and the
      // retry commits a second, equally legitimate entry - not corruption. So instead of a
      // brittle exact offset count, assert what actually matters: firstLeader converges to
      // *exactly* the same state as another broker that was never stopped.
      assertTrue(
          DockerBrokerCluster.await(
              Duration.ofSeconds(30),
              () ->
                  highestOffset(cluster, firstLeader) >= 1
                      && highestOffset(cluster, firstLeader) == highestOffset(cluster, newLeader)),
          "rejoined broker did not converge with the rest of the cluster");

      long convergedTopOffset = highestOffset(cluster, firstLeader);
      for (long offset = 0; offset <= convergedTopOffset; offset++) {
        assertEquals(
            cluster.read(newLeader, TOPIC, 0, offset).orElseThrow().payload(),
            cluster.read(firstLeader, TOPIC, 0, offset).orElseThrow().payload(),
            "content diverged at offset " + offset);
      }

      assertEquals("FOLLOWER", cluster.status(firstLeader).orElseThrow().raftState());
    }
  }

  /** Highest present offset for TOPIC/partition 0 on the given broker, or -1 if none. */
  private static long highestOffset(DockerBrokerCluster cluster, int index) {
    long highest = -1;
    for (long offset = 0; offset < 16; offset++) {
      if (cluster.read(index, TOPIC, 0, offset).isPresent()) {
        highest = offset;
      } else {
        break;
      }
    }
    return highest;
  }

  // PART 14: consumer group fetch/commit over the real HTTP API - at-least-once, no incorrect
  // redelivery after a commit, redelivery when a commit never happens.
  @Test
  void consumerGroupFetchCommitBehavesAtLeastOnceOverTheRealApi() {
    try (DockerBrokerCluster cluster = new DockerBrokerCluster(3)) {
      ContainerLogsOnFailureExtension.setActive(cluster);

      int leader = cluster.awaitLeader(Duration.ofSeconds(60));
      cluster.createTopicOnAllRunningBrokers(TOPIC, 1);

      publishWithRetry(cluster, leader, "key-1", "payload-1");
      publishWithRetry(cluster, leader, "key-2", "payload-2");

      // Simulate a processing failure: fetch, but never commit.
      MessageResponse first = cluster.fetch(leader, "group-a", TOPIC, 0).orElseThrow();
      assertEquals("payload-1", first.payload());

      // No commit happened - the same message must be redelivered.
      MessageResponse redelivered = cluster.fetch(leader, "group-a", TOPIC, 0).orElseThrow();
      assertEquals("payload-1", redelivered.payload());
      assertEquals(first.offset(), redelivered.offset());

      // Now process successfully and commit.
      cluster.commit(leader, "group-a", TOPIC, 0, first.offset() + 1);

      MessageResponse second = cluster.fetch(leader, "group-a", TOPIC, 0).orElseThrow();
      assertEquals("payload-2", second.payload());

      // A different, brand-new consumer group has its own independent offset.
      MessageResponse fromFreshGroup = cluster.fetch(leader, "group-b", TOPIC, 0).orElseThrow();
      assertEquals("payload-1", fromFreshGroup.payload());
    }
  }

  /**
   * A real client publishing against a real, occasionally-churning cluster is expected to retry on
   * NotLeaderHttpException/QuorumUnavailableHttpException and rediscover the leader - propose()
   * deliberately does not retry itself (see RaftReplicatedQueue's own javadoc), and this
   * implementation has no PreVote extension, so a freshly-elected leader can occasionally see one
   * transient hiccup. Bounded, not a blind retry loop.
   */
  private static MessageResponse publishWithRetry(
      DockerBrokerCluster cluster, int brokerIndex, String key, String payload) {
    int target = brokerIndex;
    RuntimeException lastFailure = null;
    for (int attempt = 0; attempt < 30; attempt++) {
      try {
        return cluster.publish(target, TOPIC, key, payload);
      } catch (QuorumUnavailableHttpException exception) {
        lastFailure = exception;
      } catch (RestClientException exception) {
        // A transient network hiccup (connect/read timeout, connection reset) against a real
        // container - not a leadership signal, so retry the same target without rediscovering.
        lastFailure = exception;
      } catch (NotLeaderHttpException exception) {
        lastFailure = exception;
        for (int i = 0; i < cluster.size(); i++) {
          if (cluster.isRunning(i)
              && cluster.status(i).map(s -> "LEADER".equals(s.raftState())).orElse(false)) {
            target = i;
            break;
          }
        }
      }
      try {
        Thread.sleep(300);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw lastFailure;
      }
    }
    throw lastFailure;
  }
}
