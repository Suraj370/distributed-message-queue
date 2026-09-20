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
import org.openjdk.jmh.annotations.Warmup;

/**
 * Baseline benchmark for {@link Partition#append(String, String)}, the core write path shared by
 * every producer request before Raft or the network are involved.
 *
 * <p><b>What this measures:</b> one full call to {@code Partition.append(key, payload)} against a
 * real, file-backed {@link Wal} - message construction, the WAL's synchronous, {@code fsync}'d disk
 * write, and the in-memory index updates ({@code ConcurrentLinkedQueue} offer, {@code
 * ConcurrentSkipListMap} put). This is deliberately the real append path as it exists in the
 * codebase today: {@code Partition} has no in-memory-only mode, so a benchmark that stubbed out the
 * WAL would be measuring an artificial path that no production request ever takes. The fsync is
 * usually the dominant cost here, and that is the point of the baseline - it tells you what a
 * single durable append actually costs before any concurrency or replication is layered on top.
 *
 * <p><b>What this does NOT measure:</b> Raft replication or consensus, the HTTP transport between
 * brokers, Spring application startup, concurrent producers (single-threaded, one append at a
 * time), consumer/poll paths, or latency percentiles (only mean throughput/time are reported -
 * percentile analysis is a deliberately separate, later piece of work).
 *
 * <p><b>How to run:</b> {@code ./gradlew jmh} runs every benchmark in this source set; to run only
 * this class, {@code ./gradlew jmh -Pjmh.include=PartitionAppendBenchmark}. Each of the 2 forks
 * runs 5 x 1s warmup iterations (to let the JIT compile hot paths and let the OS file cache reach a
 * steady state before results are recorded) followed by 5 x 1s measurement iterations, and the two
 * forks' results are combined - multiple forks guard against one JVM's results being skewed by
 * unlucky JIT/GC timing.
 *
 * <p><b>How to interpret the result:</b> {@code Throughput} is reported in ops/ms - the number of
 * durable appends this partition can sustain per millisecond of wall-clock time, single-threaded.
 * {@code AverageTime} (also reported, from the same measurement) is 1/throughput expressed as ms/op
 * - the average latency of one durable append. Both are shown because throughput answers "how many
 * appends/sec can this sustain" while average time answers "how long does one append take," which
 * is the more natural framing when the dominant cost is a single fsync.
 */
@State(Scope.Thread)
@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(2)
public class PartitionAppendBenchmark {

  // Fixed key/payload so every invocation does the same amount of work - varying the content
  // per call would fold uncontrolled string-generation cost into a measurement that's meant to
  // isolate Partition.append() itself, not the cost of producing test data.
  private static final String KEY = "benchmark-key";
  private static final String PAYLOAD = "x".repeat(128);

  private Path tempDir;
  private Partition partition;

  @Setup(Level.Trial)
  public void setUp() throws IOException {
    tempDir = Files.createTempDirectory("dmq-jmh-partition-append");
    Wal wal = new Wal(tempDir.resolve("partition-0.log"));
    partition = new Partition("benchmark-topic", 0, wal);
  }

  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    try (var paths = Files.walk(tempDir)) {
      paths.sorted(Comparator.reverseOrder()).forEach(PartitionAppendBenchmark::deleteQuietly);
    }
  }

  /**
   * Returning the {@link Message} is what prevents JIT dead-code elimination here: JMH consumes a
   * benchmark method's return value automatically, so an explicit {@code Blackhole} would be
   * redundant. (It would also be redundant on correctness grounds - {@code append()}'s WAL write
   * and offset-map insert are real, externally-visible side effects the JIT cannot eliminate
   * regardless of what happens to the return value.)
   */
  @Benchmark
  public Message appendMessage() throws IOException {
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
