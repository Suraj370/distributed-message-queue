package com.surajpanda.dmq.docker;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Ports;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.ToStringConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

/**
 * Boots a real N-broker cluster as N actual Docker containers (built from this project's own
 * Dockerfile - the real, production application image, not a mock), on a real Testcontainers Docker
 * network, talking to each other by their network hostnames - never localhost. All interaction from
 * the test side goes over the real HTTP API (see StatusController, MessageController,
 * ConsumerController); this class never reaches into broker internals.
 *
 * <p>The image is built once per JVM (Docker layer caching also makes repeat builds fast even
 * across JVMs) and shared by every cluster instance - only the containers themselves are
 * per-cluster, so each test gets fully isolated containers/network/data while paying the image
 * build cost only once.
 *
 * <p>stop()/restart() use the raw Docker client directly (not GenericContainer's own stop(), which
 * is designed to remove the container) so a "restart" is a genuine docker stop + start of the same
 * container - its writable filesystem (and therefore its data directory) survives, exactly like a
 * real broker process restart.
 *
 * <p>Docker can (and, observed on Docker Desktop for Windows, reliably does) assign a *different*
 * random host port for a dynamically-published container port (-p 0:8080) when that same container
 * is stopped and started again - the mapping is not guaranteed stable across a restart the way it
 * is for the container's whole lifetime otherwise. Testcontainers' own getMappedPort() caches the
 * value from the original start and has no reason to know it changed, so restart() re-inspects the
 * container directly afterward and this class tracks the current mapped port itself rather than
 * trusting that cache.
 */
public final class DockerBrokerCluster implements AutoCloseable {

  public record Timing(
      long electionMinMillis,
      long electionMaxMillis,
      long heartbeatIntervalMillis,
      long connectTimeoutMillis,
      long readTimeoutMillis) {

    public static Timing fastForTests() {
      return new Timing(2000, 3000, 300, 1500, 3000);
    }
  }

  private static volatile String sharedImageName;

  private final Network network = Network.newNetwork();
  private final int size;
  private final String[] aliases;
  private final GenericContainer<?>[] containers;
  private final ToStringConsumer[] logs;
  private final boolean[] running;
  private final int[] mappedPorts;
  private final RestClient restClient;

  public DockerBrokerCluster(int size) {
    this(size, Timing.fastForTests());
  }

  public DockerBrokerCluster(int size, Timing timing) {
    this.size = size;
    this.aliases = new String[size];
    for (int i = 0; i < size; i++) {
      aliases[i] = "broker-" + i;
    }

    SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
    requestFactory.setConnectTimeout(3000);
    requestFactory.setReadTimeout(5000);
    this.restClient = RestClient.builder().requestFactory(requestFactory).build();

    String image = sharedImage();

    this.containers = new GenericContainer<?>[size];
    this.logs = new ToStringConsumer[size];
    this.running = new boolean[size];
    this.mappedPorts = new int[size];

    for (int i = 0; i < size; i++) {
      containers[i] = buildContainer(image, i, timing);
      logs[i] = new ToStringConsumer();
      containers[i].withLogConsumer(logs[i]);
    }

    Startables.deepStart(List.of(containers)).join();
    for (int i = 0; i < size; i++) {
      running[i] = true;
      mappedPorts[i] = containers[i].getMappedPort(8080);
    }
  }

  private GenericContainer<?> buildContainer(String image, int index, Timing timing) {

    GenericContainer<?> container =
        new GenericContainer<>(DockerImageName.parse(image))
            .withNetwork(network)
            .withNetworkAliases(aliases[index])
            .withExposedPorts(8080)
            .withEnv("BROKER_ID", aliases[index])
            .withEnv("BROKER_HOST", aliases[index])
            .withEnv("BROKER_PORT", "8080")
            .withEnv("SERVER_PORT", "8080")
            .withEnv("BROKER_DATA_DIRECTORY", "/data/" + aliases[index])
            .withEnv("RAFT_ELECTION_MIN_TIMEOUT_MILLIS", String.valueOf(timing.electionMinMillis()))
            .withEnv("RAFT_ELECTION_MAX_TIMEOUT_MILLIS", String.valueOf(timing.electionMaxMillis()))
            .withEnv(
                "RAFT_HEARTBEAT_INTERVAL_MILLIS", String.valueOf(timing.heartbeatIntervalMillis()))
            .withEnv(
                "RAFT_TRANSPORT_CONNECT_TIMEOUT_MILLIS",
                String.valueOf(timing.connectTimeoutMillis()))
            .withEnv(
                "RAFT_TRANSPORT_READ_TIMEOUT_MILLIS", String.valueOf(timing.readTimeoutMillis()))
            .waitingFor(
                Wait.forHttp("/actuator/health")
                    .forPort(8080)
                    .forStatusCode(200)
                    .withStartupTimeout(Duration.ofSeconds(120)));

    for (int i = 0; i < size; i++) {
      String prefix = "CLUSTER_BROKERS_" + i + "_";
      container
          .withEnv(prefix + "ID", aliases[i])
          .withEnv(prefix + "HOST", aliases[i])
          .withEnv(prefix + "PORT", "8080");
    }

    return container;
  }

  private static final String IMAGE_TAG = "dmq-broker-dockertest:latest";

  /**
   * Builds the image via a plain `docker build` (ProcessBuilder), not Testcontainers'
   * ImageFromDockerfile - the latter assembles its own build context by walking/taring individual
   * paths, which on this project's Windows-checkout Gradle wrapper produced a build that failed
   * running gradlew inside the container for reasons that did not reproduce with a normal `docker
   * build .` using Docker's own native context handling. Shelling out to the real Docker CLI from
   * the project root is simply the same thing a developer would run by hand, and is what this
   * class's Dockerfile is validated against.
   */
  private static synchronized String sharedImage() {
    if (sharedImageName != null) {
      return sharedImageName;
    }

    Path projectRoot = Path.of(System.getProperty("user.dir"));

    try {
      Process build =
          new ProcessBuilder("docker", "build", "-t", IMAGE_TAG, ".")
              .directory(projectRoot.toFile())
              .redirectErrorStream(true)
              .start();

      StringBuilder output = new StringBuilder();
      try (var reader =
          new java.io.BufferedReader(new java.io.InputStreamReader(build.getInputStream()))) {
        String line;
        while ((line = reader.readLine()) != null) {
          output.append(line).append('\n');
        }
      }

      int exitCode = build.waitFor();
      if (exitCode != 0) {
        throw new IllegalStateException(
            "docker build failed with exit code " + exitCode + ":\n" + output);
      }

    } catch (java.io.IOException | InterruptedException exception) {
      if (exception instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new IllegalStateException("Failed to build the broker Docker image", exception);
    }

    sharedImageName = IMAGE_TAG;
    return sharedImageName;
  }

  public int size() {
    return size;
  }

  public String id(int index) {
    return aliases[index];
  }

  public boolean isRunning(int index) {
    return running[index];
  }

  private String baseUrl(int index) {
    return "http://" + containers[index].getHost() + ":" + mappedPorts[index];
  }

  public Optional<StatusResponse> status(int index) {
    if (!running[index]) {
      return Optional.empty();
    }
    try {
      return Optional.ofNullable(
          restClient
              .get()
              .uri(baseUrl(index) + "/api/v1/status")
              .retrieve()
              .body(StatusResponse.class));
    } catch (RuntimeException exception) {
      return Optional.empty();
    }
  }

  public void createTopic(int index, String name, int partitions) {
    restClient
        .post()
        .uri(
            baseUrl(index) + "/api/v1/topics?name={name}&partitions={partitions}", name, partitions)
        .retrieve()
        .toBodilessEntity();
  }

  public void createTopicOnAllRunningBrokers(String name, int partitions) {
    for (int i = 0; i < size; i++) {
      if (running[i]) {
        createTopic(i, name, partitions);
      }
    }
  }

  public MessageResponse publish(int index, String topic, String key, String payload) {
    try {
      return restClient
          .post()
          .uri(
              baseUrl(index) + "/api/v1/messages?topic={topic}&key={key}&payload={payload}",
              topic,
              key,
              payload)
          .retrieve()
          .body(MessageResponse.class);
    } catch (HttpClientErrorException.Conflict exception) {
      throw new NotLeaderHttpException(id(index) + " is not the leader");
    } catch (HttpServerErrorException.ServiceUnavailable exception) {
      throw new QuorumUnavailableHttpException(id(index) + " could not reach a majority");
    }
  }

  /**
   * Used heavily inside bounded polling loops (awaiting a message to appear), so a transient
   * network hiccup against a real container is treated the same as "not there yet" rather than
   * aborting the whole poll - the next iteration simply tries again.
   */
  public Optional<MessageResponse> read(int index, String topic, int partition, long offset) {
    try {
      return Optional.ofNullable(
          restClient
              .get()
              .uri(
                  baseUrl(index)
                      + "/api/v1/messages?topic={topic}&partition={partition}&offset={offset}",
                  topic,
                  partition,
                  offset)
              .retrieve()
              .body(MessageResponse.class));
    } catch (HttpClientErrorException.NotFound exception) {
      return Optional.empty();
    } catch (RestClientException exception) {
      return Optional.empty();
    }
  }

  public Optional<MessageResponse> fetch(int index, String group, String topic, int partition) {
    try {
      return Optional.ofNullable(
          restClient
              .post()
              .uri(
                  baseUrl(index)
                      + "/api/v1/consumer-groups/{group}/fetch?topic={topic}&partition={partition}",
                  group,
                  topic,
                  partition)
              .retrieve()
              .body(MessageResponse.class));
    } catch (HttpClientErrorException.NotFound exception) {
      return Optional.empty();
    }
  }

  public void commit(int index, String group, String topic, int partition, long offset) {
    restClient
        .post()
        .uri(
            baseUrl(index)
                + "/api/v1/consumer-groups/{group}/commit?topic={topic}&partition={partition}&offset={offset}",
            group,
            topic,
            partition,
            offset)
        .retrieve()
        .toBodilessEntity();
  }

  /** Real docker stop, not GenericContainer.stop() (which removes the container). */
  public void stop(int index) {
    if (!running[index]) {
      return;
    }
    GenericContainer<?> container = containers[index];
    container.getDockerClient().stopContainerCmd(container.getContainerId()).exec();
    running[index] = false;
  }

  /** Real docker start of the SAME container - its data directory survives. */
  public void restart(int index) {
    GenericContainer<?> container = containers[index];
    container.getDockerClient().startContainerCmd(container.getContainerId()).exec();
    running[index] = true;
    mappedPorts[index] = refreshMappedPort(container);

    boolean healthy =
        await(
            Duration.ofSeconds(60),
            () -> {
              try {
                restClient
                    .get()
                    .uri(baseUrl(index) + "/actuator/health")
                    .retrieve()
                    .toBodilessEntity();
                return true;
              } catch (RuntimeException exception) {
                return false;
              }
            });

    if (!healthy) {
      throw new AssertionError(id(index) + " did not become healthy again after restart");
    }
  }

  /**
   * Re-inspects the container's actual current port binding for 8080/tcp - see the class javadoc on
   * why the port published at the original start() cannot be trusted after a restart. Docker does
   * not always have the binding populated in the very first inspect right after startContainerCmd()
   * returns, so this polls briefly rather than failing on the first miss.
   */
  private static int refreshMappedPort(GenericContainer<?> container) {

    long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();

    while (true) {
      InspectContainerResponse inspection =
          container.getDockerClient().inspectContainerCmd(container.getContainerId()).exec();
      Ports.Binding[] bindings =
          inspection.getNetworkSettings().getPorts().getBindings().get(ExposedPort.tcp(8080));

      if (bindings != null && bindings.length > 0) {
        return Integer.parseInt(bindings[0].getHostPortSpec());
      }

      if (System.nanoTime() >= deadline) {
        throw new IllegalStateException(
            "No host port binding found for 8080/tcp on container "
                + container.getContainerId()
                + " within 10s of restarting");
      }

      try {
        Thread.sleep(100);
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted while waiting for port binding", exception);
      }
    }
  }

  public int awaitLeader(Duration timeout) {
    int[] found = {-1};
    boolean elected =
        await(
            timeout,
            () -> {
              int leaderIndex = -1;
              int leaderCount = 0;
              for (int i = 0; i < size; i++) {
                Optional<StatusResponse> status = status(i);
                if (status.isPresent() && "LEADER".equals(status.get().raftState())) {
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

  public int awaitLeaderExcluding(int excluded, Duration timeout) {
    int[] found = {-1};
    boolean elected =
        await(
            timeout,
            () -> {
              for (int i = 0; i < size; i++) {
                if (i == excluded) {
                  continue;
                }
                Optional<StatusResponse> status = status(i);
                if (status.isPresent() && "LEADER".equals(status.get().raftState())) {
                  found[0] = i;
                  return true;
                }
              }
              return false;
            });
    if (!elected) {
      throw new AssertionError(
          "No new leader emerged (excluding " + id(excluded) + ") within " + timeout);
    }
    return found[0];
  }

  public static boolean await(Duration timeout, BooleanSupplier condition) {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      if (condition.getAsBoolean()) {
        return true;
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    return condition.getAsBoolean();
  }

  /** For failure diagnostics - see ContainerLogsOnFailureExtension. */
  public List<String> describeForDiagnostics() {
    List<String> lines = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      Optional<StatusResponse> status = running[i] ? status(i) : Optional.empty();
      lines.add(
          id(i)
              + ": running="
              + running[i]
              + ", status="
              + status.map(Object::toString).orElse("<unreachable>"));
    }
    return lines;
  }

  public String logsFor(int index) {
    return logs[index].toUtf8String();
  }

  @Override
  public void close() {
    for (GenericContainer<?> container : containers) {
      try {
        container
            .stop(); // full teardown (stop + remove) at the end of a test - not restart()'s job
      } catch (RuntimeException ignored) {
        // best-effort cleanup
      }
    }
    network.close();
  }
}
