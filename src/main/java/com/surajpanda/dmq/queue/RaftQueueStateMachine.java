package com.surajpanda.dmq.queue;

import com.surajpanda.dmq.message.Message;
import com.surajpanda.dmq.partition.Partition;
import com.surajpanda.dmq.raft.LogEntry;
import com.surajpanda.dmq.raft.RaftStateMachine;
import com.surajpanda.dmq.topic.Topic;
import com.surajpanda.dmq.topic.TopicManager;
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
 * <p>Durably tracks its own applied index (separately from RaftNode's in-memory lastApplied) so a
 * restart can never cause the same committed entry to be applied - and therefore appended to the
 * WAL - twice.
 */
public class RaftQueueStateMachine implements RaftStateMachine {

  private final TopicManager topicManager;
  private final LastAppliedStore lastAppliedStore;
  private final Map<Long, Message> appliedMessages = new ConcurrentHashMap<>();

  private long lastAppliedIndex;

  public RaftQueueStateMachine(TopicManager topicManager, LastAppliedStore lastAppliedStore) {
    this.topicManager = topicManager;
    this.lastAppliedStore = lastAppliedStore;
    this.lastAppliedIndex = lastAppliedStore.load();
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
      Message applied = partition.append(command.key(), command.payload());
      appliedMessages.put(entry.index(), applied);
    } catch (IOException exception) {
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
