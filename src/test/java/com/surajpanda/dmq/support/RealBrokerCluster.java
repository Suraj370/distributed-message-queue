package com.surajpanda.dmq.support;

import com.surajpanda.dmq.DistributedMessageQueueApplication;
import com.surajpanda.dmq.broker.Broker;
import com.surajpanda.dmq.raft.RaftNode;
import com.surajpanda.dmq.raft.RaftState;
import com.surajpanda.dmq.topic.TopicManager;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Boots a real N-broker cluster as N actual Spring Boot application contexts, each with its own
 * embedded HTTP server on its own port, wired through the real HTTP Raft transport - unlike
 * SimulatedCluster (in-process, no networking), this exercises the genuine multi-process wiring in
 * RaftConfiguration end to end. Cluster membership is fixed at construction (ports are chosen once
 * and configured into every broker's cluster.brokers list), matching the "configured membership is
 * authoritative" requirement even while a broker is stopped/restarted.
 */
public final class RealBrokerCluster implements AutoCloseable {

  public record Timing(
      long electionMinMillis,
      long electionMaxMillis,
      long heartbeatIntervalMillis,
      long connectTimeoutMillis,
      long readTimeoutMillis) {

    public static Timing fastForTests() {
      return new Timing(600, 1000, 100, 500, 1000);
    }
  }

  private final ConfigurableApplicationContext[] contexts;
  private final int[] ports;
  private final Path baseDir;
  private final Timing timing;

  public RealBrokerCluster(int size, Path baseDir) {
    this(size, baseDir, Timing.fastForTests());
  }

  public RealBrokerCluster(int size, Path baseDir, Timing timing) {
    this.baseDir = baseDir;
    this.timing = timing;
    this.ports = findFreePorts(size);
    this.contexts = new ConfigurableApplicationContext[size];
    for (int i = 0; i < size; i++) {
      contexts[i] = startBroker(i);
    }
  }

  public int size() {
    return ports.length;
  }

  public String id(int index) {
    return "broker-" + index;
  }

  public <T> T bean(int index, Class<T> type) {
    ConfigurableApplicationContext context = contexts[index];
    if (context == null) {
      throw new IllegalStateException(id(index) + " is not running");
    }
    return context.getBean(type);
  }

  public Broker broker(int index) {
    return bean(index, Broker.class);
  }

  public RaftNode raftNode(int index) {
    return bean(index, RaftNode.class);
  }

  public TopicManager topics(int index) {
    return bean(index, TopicManager.class);
  }

  public void createTopicOnAllRunningBrokers(String name, int partitions) throws IOException {
    for (int i = 0; i < size(); i++) {
      if (contexts[i] != null) {
        topics(i).createTopic(name, partitions);
      }
    }
  }

  public boolean isRunning(int index) {
    return contexts[index] != null;
  }

  /** Simulates a crash: closes the context, releasing its port, without discarding its files. */
  public void stop(int index) {
    ConfigurableApplicationContext context = contexts[index];
    if (context != null) {
      context.close();
      contexts[index] = null;
    }
  }

  /**
   * Simulates a process restart: same broker id, data directory and port, fresh in-memory state.
   */
  public void restart(int index) {
    stop(index);
    contexts[index] = startBroker(index);
  }

  /**
   * Polls every running broker for RaftState.LEADER until exactly one is found or the timeout
   * elapses - a bounded, explicit readiness wait rather than a blind sleep.
   */
  public int awaitLeader(Duration timeout) {
    int[] found = new int[1];
    boolean elected =
        await(
            timeout,
            () -> {
              int leaderIndex = -1;
              int leaderCount = 0;
              for (int i = 0; i < size(); i++) {
                if (contexts[i] != null && raftNode(i).getState() == RaftState.LEADER) {
                  leaderIndex = i;
                  leaderCount++;
                }
              }
              found[0] = leaderIndex;
              return leaderCount == 1;
            });
    if (!elected) {
      throw new AssertionError("No single leader emerged within " + timeout);
    }
    return found[0];
  }

  /** Generic bounded poll: checks condition every 20ms until true or timeout elapses. */
  public static boolean await(Duration timeout, BooleanSupplier condition) {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      if (condition.getAsBoolean()) {
        return true;
      }
      try {
        Thread.sleep(20);
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    return condition.getAsBoolean();
  }

  @Override
  public void close() {
    for (int i = 0; i < size(); i++) {
      stop(i);
    }
  }

  private ConfigurableApplicationContext startBroker(int index) {

    Path dataDir = baseDir.resolve(id(index));
    List<String> args = new ArrayList<>();

    // Command-line-style args (highest property-source precedence) so these genuinely override
    // application.yml's defaults (server.port: ${broker.port}, which defaults to 8080) instead of
    // being overridden by them - SpringApplicationBuilder.properties(...) adds its values as
    // *default* properties, the lowest-precedence source, which is not what a per-instance
    // override needs here.
    args.add("--broker.id=" + id(index));
    args.add("--broker.host=localhost");
    args.add("--broker.port=" + ports[index]);
    args.add("--broker.data-directory=" + dataDir);
    args.add("--server.port=" + ports[index]);
    args.add("--spring.main.banner-mode=off");

    for (int i = 0; i < ports.length; i++) {
      args.add("--cluster.brokers[" + i + "].id=" + id(i));
      args.add("--cluster.brokers[" + i + "].host=localhost");
      args.add("--cluster.brokers[" + i + "].port=" + ports[i]);
    }

    args.add("--raft.election.min-timeout-millis=" + timing.electionMinMillis());
    args.add("--raft.election.max-timeout-millis=" + timing.electionMaxMillis());
    args.add("--raft.heartbeat-interval-millis=" + timing.heartbeatIntervalMillis());
    args.add("--raft.transport.connect-timeout-millis=" + timing.connectTimeoutMillis());
    args.add("--raft.transport.read-timeout-millis=" + timing.readTimeoutMillis());

    return new SpringApplicationBuilder(DistributedMessageQueueApplication.class)
        .run(args.toArray(new String[0]));
  }

  private static int[] findFreePorts(int count) {
    java.net.ServerSocket[] sockets = new java.net.ServerSocket[count];
    try {
      int[] result = new int[count];
      for (int i = 0; i < count; i++) {
        sockets[i] = new java.net.ServerSocket(0);
        result[i] = sockets[i].getLocalPort();
      }
      return result;
    } catch (IOException exception) {
      throw new java.io.UncheckedIOException(exception);
    } finally {
      for (java.net.ServerSocket socket : sockets) {
        if (socket != null) {
          try {
            socket.close();
          } catch (IOException ignored) {
            // best-effort release
          }
        }
      }
    }
  }
}
