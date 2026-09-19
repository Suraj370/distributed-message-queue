package com.surajpanda.dmq.raft;

import java.time.Clock;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Thin real-time wiring around RaftElectionDriver and HeartbeatBroadcaster. This is the only Raft
 * class that owns a thread: a single daemon scheduled-executor thread ticks at the heartbeat
 * interval, checking the election timeout and sending heartbeats when leader. All the actual Raft
 * decision logic stays in the deterministic, time-injected classes so it can be unit tested without
 * this scheduler ever running.
 *
 * <p>Enforces heartbeatIntervalMillis &lt; minElectionTimeoutMillis, per the Raft requirement that
 * a leader must be able to heartbeat multiple times within a follower's election timeout.
 */
public class RaftNodeScheduler implements AutoCloseable {

  private final RaftElectionDriver electionDriver;
  private final HeartbeatBroadcaster heartbeatBroadcaster;
  private final Clock clock;
  private final long tickIntervalMillis;
  private final ScheduledExecutorService executor;
  private volatile ScheduledFuture<?> tickTask;

  public RaftNodeScheduler(
      RaftElectionDriver electionDriver,
      HeartbeatBroadcaster heartbeatBroadcaster,
      long minElectionTimeoutMillis,
      long heartbeatIntervalMillis,
      Clock clock) {

    if (heartbeatIntervalMillis >= minElectionTimeoutMillis) {
      throw new IllegalArgumentException(
          "heartbeatIntervalMillis must be less than minElectionTimeoutMillis: heartbeatIntervalMillis="
              + heartbeatIntervalMillis
              + ", minElectionTimeoutMillis="
              + minElectionTimeoutMillis);
    }

    this.electionDriver = electionDriver;
    this.heartbeatBroadcaster = heartbeatBroadcaster;
    this.clock = clock;
    this.tickIntervalMillis = heartbeatIntervalMillis;
    this.executor = Executors.newSingleThreadScheduledExecutor(RaftNodeScheduler::newDaemonThread);
  }

  private static Thread newDaemonThread(Runnable runnable) {
    Thread thread = new Thread(runnable, "raft-node-scheduler");
    thread.setDaemon(true);
    return thread;
  }

  public void start() {
    tickTask =
        executor.scheduleAtFixedRate(this::tick, 0, tickIntervalMillis, TimeUnit.MILLISECONDS);
  }

  private void tick() {
    try {
      electionDriver.tick(clock.millis());
      heartbeatBroadcaster.broadcastIfLeader();
    } catch (RuntimeException exception) {
      // A scheduled task that throws would silently stop being rescheduled; swallow and retry
      // on the next tick instead of killing the loop.
    }
  }

  @Override
  public void close() {

    ScheduledFuture<?> task = tickTask;
    if (task != null) {
      task.cancel(false);
    }

    executor.shutdownNow();

    try {
      executor.awaitTermination(1, TimeUnit.SECONDS);
    } catch (InterruptedException interruptedException) {
      Thread.currentThread().interrupt();
    }
  }
}
