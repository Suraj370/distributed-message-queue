package com.surajpanda.dmq.benchmarks;

import com.surajpanda.dmq.message.Message;
import com.surajpanda.dmq.partition.Partition;
import com.surajpanda.dmq.wal.Wal;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Concurrent-producer benchmark for {@link Partition#append(String, String)}. This is a
 * <b>concurrent durability benchmark</b>, not a pure in-memory queue benchmark: every worker thread
 * appends to the same {@link Partition}, backed by the same real, {@code fsync}'d {@link Wal},
 * exactly as concurrent producers on the same partition do in production today. {@code Partition}'s
 * synchronization (a single {@code ReentrantLock} guarding the WAL write and the in-memory index
 * updates) and the WAL's own synchronous fsync are used completely unchanged - this benchmark
 * observes the existing append path under contention, it does not alter it.
 *
 * <p><b>Why one shared {@code Partition} across threads:</b> {@link SharedPartitionState} is
 * {@code @State(Scope.Benchmark)}, meaning every worker thread in a trial shares one {@code
 * Partition}/{@code Wal} instance. That shared, mutable state is intentional here - it is the
 * entire point of the benchmark. A per-thread {@code Partition} would eliminate the lock and fsync
 * contention this benchmark exists to observe, and would silently turn it back into N independent
 * copies of the single-threaded baseline.
 *
 * <p><b>Why four {@code @Benchmark} methods instead of {@code @Param}:</b> JMH's thread count is a
 * harness-level execution setting (how many worker threads call the benchmark method), not a value
 * visible to benchmark code, so it cannot be swept with {@code @Param} the way a normal input can.
 * The standard JMH equivalent - used here - is one {@code @Benchmark} method per fixed
 * {@code @Threads(N)} value, all sharing the same {@code @State(Scope.Benchmark)} object, so 1, 2,
 * 4 and 8 producers are each a genuine, separately-measured JMH benchmark run rather than a
 * parameterized re-invocation of the same one.
 *
 * <p><b>What this measures:</b> throughput and average latency of {@code Partition.append(key,
 * payload)} under 1, 2, 4 and 8 concurrent producer threads, including lock acquisition and the
 * WAL's synchronous disk write/fsync while the lock is held.
 *
 * <p><b>What this does NOT measure:</b> Raft replication or consensus, the HTTP transport between
 * brokers, Spring application startup, consumer/poll paths, or latency percentiles (mean
 * throughput/time only). It also does not measure a redesigned or optimized append path - {@code
 * Partition}'s existing single-lock synchronization is left exactly as it is.
 *
 * <p><b>Expected shape of the result:</b> because every append - regardless of thread count -
 * passes through the same single {@code ReentrantLock} with a synchronous fsync inside the critical
 * section, the append path is already fully serialized. Throughput scaling with thread count is
 * therefore not expected; flat or declining throughput as threads increase would indicate the
 * lock/fsync is the bottleneck, which is exactly what this benchmark is built to reveal - it is a
 * measurement, not something this change attempts to fix.
 *
 * <p><b>How to run:</b> {@code ./gradlew jmh -Pjmh.include=ConcurrentPartitionAppendBenchmark}.
 * Configuration mirrors {@link PartitionAppendBenchmark} for comparability: 2 forks, 5 x 1s warmup
 * iterations, 5 x 1s measurement iterations, {@code Throughput} (ops/ms) and {@code AverageTime}
 * (ms/op) both reported. Each of the four thread-count variants is its own fork sequence, so a
 * fresh, empty {@code Partition}/{@code Wal} temp directory is used per variant (see {@link
 * #setUp()}), keeping earlier variants' WAL growth from skewing later ones.
 */
@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(2)
public class ConcurrentPartitionAppendBenchmark {

  @State(Scope.Benchmark)
  public static class SharedPartitionState {

    // Fixed key/payload, same as the single-threaded baseline - every invocation on every thread
    // does the same amount of work, so the measurement isolates append()'s own synchronization
    // and fsync cost rather than per-thread content-generation cost.
    private static final String KEY = "benchmark-key";
    private static final String PAYLOAD = "x".repeat(128);

    private Path tempDir;
    private Partition partition;

    @Setup(Level.Trial)
    public void setUp() throws IOException {
      tempDir = Files.createTempDirectory("dmq-jmh-concurrent-partition-append");
      Wal wal = new Wal(tempDir.resolve("partition-0.log"));
      partition = new Partition("concurrent-benchmark-topic", 0, wal);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
      try (var paths = Files.walk(tempDir)) {
        paths.sorted(Comparator.reverseOrder()).forEach(SharedPartitionState::deleteQuietly);
      }
    }

    Message append() throws IOException {
      return partition.append(KEY, PAYLOAD);
    }

    private static void deleteQuietly(Path path) {
      try {
        Files.deleteIfExists(path);
      } catch (IOException ignored) {
        // best-effort cleanup of the benchmark's own temp directory
      }
    }
  }

  /**
   * Single-producer baseline for this class - directly comparable to {@link
   * PartitionAppendBenchmark#appendMessage()}, but going through {@code Scope.Benchmark} state
   * instead of {@code Scope.Thread}, so any gap between the two isolates the cost of that scope
   * change rather than genuine contention (there is none at 1 thread).
   */
  @Benchmark
  @Threads(1)
  public Message appendWith1Producer(SharedPartitionState state) throws IOException {
    return state.append();
  }

  @Benchmark
  @Threads(2)
  public Message appendWith2Producers(SharedPartitionState state) throws IOException {
    return state.append();
  }

  @Benchmark
  @Threads(4)
  public Message appendWith4Producers(SharedPartitionState state) throws IOException {
    return state.append();
  }

  @Benchmark
  @Threads(8)
  public Message appendWith8Producers(SharedPartitionState state) throws IOException {
    return state.append();
  }
}
