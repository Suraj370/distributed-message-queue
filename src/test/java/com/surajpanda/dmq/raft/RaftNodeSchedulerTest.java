package com.surajpanda.dmq.raft;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class RaftNodeSchedulerTest {

  @Test
  void shouldRejectHeartbeatIntervalNotLessThanElectionTimeout() {
    RaftNode node = new RaftNode("broker-1");
    RaftElectionDriver driver =
        new RaftElectionDriver(
            node, new ElectionCoordinator(node, List.of(), 1), electionTimeout(), 0);
    HeartbeatBroadcaster broadcaster = new HeartbeatBroadcaster(node, List.of());

    assertThrows(
        IllegalArgumentException.class,
        () -> new RaftNodeScheduler(driver, broadcaster, 100, 100, Clock.systemUTC()));
  }

  @Test
  void shouldSendHeartbeatsPeriodicallyWhileLeaderWithoutLeakingThreads()
      throws InterruptedException {
    RaftNode leader = new RaftNode("broker-1");
    leader.becomeCandidate();
    leader.becomeLeader();

    RaftNode follower = new RaftNode("broker-2");
    CountDownLatch receivedThreeHeartbeats = new CountDownLatch(3);

    HeartbeatConnection countingConnection =
        heartbeat -> {
          HeartbeatResponse response = follower.handleHeartbeat(heartbeat);
          receivedThreeHeartbeats.countDown();
          return response;
        };

    HeartbeatBroadcaster broadcaster =
        new HeartbeatBroadcaster(
            leader, List.of(new HeartbeatPeer("broker-2", countingConnection)));

    RaftElectionDriver driver =
        new RaftElectionDriver(
            leader, new ElectionCoordinator(leader, List.of(), 1), electionTimeout(), 0);

    RaftNodeScheduler scheduler =
        new RaftNodeScheduler(driver, broadcaster, 5_000, 20, Clock.systemUTC());

    try {
      scheduler.start();
      assertTrue(receivedThreeHeartbeats.await(5, TimeUnit.SECONDS));
      assertTrue(schedulerThreadIsRunning());
    } finally {
      scheduler.close();
    }

    long deadline = System.currentTimeMillis() + 2000;
    while (schedulerThreadIsRunning() && System.currentTimeMillis() < deadline) {
      Thread.onSpinWait();
    }

    assertTrue(!schedulerThreadIsRunning());
  }

  private static boolean schedulerThreadIsRunning() {
    return Thread.getAllStackTraces().keySet().stream()
        .anyMatch(thread -> "raft-node-scheduler".equals(thread.getName()) && thread.isAlive());
  }

  private static ElectionTimeout electionTimeout() {
    return new ElectionTimeout(5_000, 5_000, new java.util.Random(1));
  }
}
