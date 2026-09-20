package com.surajpanda.dmq.raft.transport;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Timing and transport tuning for the real broker-to-broker Raft wiring. Cluster membership itself
 * stays in ClusterProperties/BrokerAddress - this only covers how this broker paces its own
 * election timeout/heartbeats and how patient it is with a single HTTP call to a peer.
 */
@Validated
@ConfigurationProperties(prefix = "raft")
public record RaftProperties(
    @NotNull @Valid ElectionTiming election,
    @Positive long heartbeatIntervalMillis,
    @NotNull @Valid Transport transport) {

  /**
   * minTimeoutMillis/maxTimeoutMillis must stay comfortably above heartbeatIntervalMillis so a
   * leader can heartbeat multiple times within a follower's timeout window, per Raft's own timing
   * requirement (enforced again, defensively, by RaftNodeScheduler itself).
   */
  public record ElectionTiming(@Positive long minTimeoutMillis, @Positive long maxTimeoutMillis) {}

  /**
   * connectTimeoutMillis/readTimeoutMillis bound how long a single outbound RequestVote/
   * AppendEntries/Heartbeat HTTP call may block on an unreachable or slow peer, so one bad peer can
   * never stall this broker's election/replication loop indefinitely.
   */
  public record Transport(@Positive long connectTimeoutMillis, @Positive long readTimeoutMillis) {}
}
