package com.surajpanda.dmq.docker;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;

/**
 * Prints each broker's container logs and last-known status only when a test actually fails -
 * successful runs stay quiet. A test registers its cluster via DockerBrokerClusterTest.setActive()
 * (a per-thread handle, since JUnit runs test methods sequentially within a class by default) at
 * the start of the test body and clears it when done.
 */
public class ContainerLogsOnFailureExtension implements TestWatcher {

  static final ThreadLocal<DockerBrokerCluster> ACTIVE = new ThreadLocal<>();

  public static void setActive(DockerBrokerCluster cluster) {
    ACTIVE.set(cluster);
  }

  public static void clearActive() {
    ACTIVE.remove();
  }

  @Override
  public void testFailed(ExtensionContext context, Throwable cause) {

    DockerBrokerCluster cluster = ACTIVE.get();
    if (cluster == null) {
      return;
    }

    System.out.println(
        "=== "
            + context.getDisplayName()
            + " FAILED - broker diagnostics ("
            + cluster.size()
            + " brokers) ===");

    for (String line : cluster.describeForDiagnostics()) {
      System.out.println(line);
    }

    for (int i = 0; i < cluster.size(); i++) {
      System.out.println("--- logs: " + cluster.id(i) + " ---");
      System.out.println(cluster.logsFor(i));
    }

    System.out.println("=== end diagnostics ===");
  }
}
