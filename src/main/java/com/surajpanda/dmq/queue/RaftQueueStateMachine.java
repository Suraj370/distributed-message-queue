package com.surajpanda.dmq.queue;

import com.surajpanda.dmq.message.Message;
import com.surajpanda.dmq.partition.Partition;
import com.surajpanda.dmq.raft.LogEntry;
import com.surajpanda.dmq.raft.RaftStateMachine;
import com.surajpanda.dmq.topic.Topic;
import com.surajpanda.dmq.topic.TopicManager;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Applies committed PublishCommand log entries to the real queue storage. This is the *only* place
 * that calls Partition.append() for a Raft-proposed publish: a command only ever reaches durable
 * queue state (WAL + in-memory) once Raft has committed it, never merely because a leader received
 * a client request. Runs identically on the leader and on every follower, so applying the same
 * committed log in the same order is what makes their queue state converge.
 *
 * <p>Durably tracks its own applied index (separately from RaftNode's in-memory lastApplied) as a
 * fast-path skip for entries already known to be applied. That index alone is not sufficient for
 * correctness, though: it is saved *after* the WAL write, so a crash between the two would leave it
 * stale and the same entry would be re-applied on restart. What actually prevents a duplicate
 * message in that case is Partition.hasAppliedRaftIndex() - the WAL itself, tagged with the Raft
 * log index, is the source of truth for whether a given committed command was already durably
 * applied.
 */
public class RaftQueueStateMachine implements RaftStateMachine {

  private final TopicManager topicManager;
  private final LastAppliedStore lastAppliedStore;
  private final MeterRegistry meterRegistry;
  private final Map<Long, Message> appliedMessages = new ConcurrentHashMap<>();

  private long lastAppliedIndex;

  public RaftQueueStateMachine(TopicManager topicManager, LastAppliedStore lastAppliedStore) {
    this(topicManager, lastAppliedStore, new SimpleMeterRegistry());
  }

  /**
   * Same as the two-arg constructor, but records WAL append activity/latency/errors into
   * meterRegistry (see {@link #apply(LogEntry)}). The two-arg constructor defaults to a throwaway
   * {@link SimpleMeterRegistry} so every existing caller (tests included) keeps working exactly as
   * before; only the real Spring-wired broker (see RaftConfiguration) needs the metrics to actually
   * go anywhere.
   */
  public RaftQueueStateMachine(
      TopicManager topicManager, LastAppliedStore lastAppliedStore, MeterRegistry meterRegistry) {
    this.topicManager = topicManager;
    this.lastAppliedStore = lastAppliedStore;
    this.lastAppliedIndex = lastAppliedStore.load();
    this.meterRegistry = meterRegistry;
  }

  @Override
  public synchronized void apply(LogEntry entry) {

    if (entry.index() <= lastAppliedIndex) {
      // Already durably applied in a previous run - applying it again would duplicate the
      // message in the WAL.
      return;
    }

    PublishCommand command = PublishCommand.decode(entry.command());

    Topic topic = topicManager.getTopic(command.topic());

    if (topic == null) {
      throw new IllegalStateException(
          "Committed PublishCommand references an unknown topic: " + command.topic());
    }

    Partition partition = topic.getPartition(command.partition());

    try {
      if (!partition.hasAppliedRaftIndex(entry.index())) {
        // Not a fast-path skip via lastAppliedIndex, but not necessarily new either: a previous
        // run may have written the WAL entry and then crashed before persisting lastAppliedIndex.
        // The WAL tag is what actually prevents the duplicate in that case.
        Timer.Sample appendTiming = Timer.start(meterRegistry);
        Message applied = partition.append(command.key(), command.payload(), entry.index());
        appendTiming.stop(
            Timer.builder("dmq_wal_append_latency_seconds")
                .description("Latency of one durable Partition.append() call, WAL fsync included")
                .tag("topic", command.topic())
                .tag("partition", String.valueOf(command.partition()))
                .register(meterRegistry));
        meterRegistry
            .counter(
                "dmq_wal_append_total",
                "topic",
                command.topic(),
                "partition",
                String.valueOf(command.partition()))
            .increment();
        appliedMessages.put(entry.index(), applied);
      }
    } catch (IOException exception) {
      meterRegistry
          .counter(
              "dmq_wal_errors_total",
              "topic",
              command.topic(),
              "partition",
              String.valueOf(command.partition()))
          .increment();
      throw new UncheckedIOException(
          "Failed to apply committed PublishCommand at index " + entry.index(), exception);
    }

    lastAppliedIndex = entry.index();
    lastAppliedStore.save(lastAppliedIndex);
  }

  /**
   * Retrieves (and forgets) the Message produced by applying the entry at logIndex, if this process
   * actually applied it just now. Empty if it was skipped as already-applied.
   */
  public Optional<Message> takeAppliedMessage(long logIndex) {
    return Optional.ofNullable(appliedMessages.remove(logIndex));
  }
}
