package com.surajpanda.dmq.queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.surajpanda.dmq.raft.LogEntry;
import com.surajpanda.dmq.topic.TopicManager;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Covers RaftQueueStateMachine's crash-consistency contract: a committed Raft entry must not
 * produce a duplicate queue message after a restart, even if the crash happened between the WAL
 * write and the lastAppliedIndex marker being persisted - see Partition.hasAppliedRaftIndex, which
 * is what actually prevents the duplicate in that window (lastAppliedIndex alone is only a
 * fast-path skip, not the correctness mechanism).
 */
class RaftQueueStateMachineRecoveryTest {

  @TempDir Path tempDir;

  /** Wraps a real LastAppliedStore and can be armed to throw on the next save(), as if a crash. */
  private static final class FailingLastAppliedStore implements LastAppliedStore {
    private final LastAppliedStore delegate;
    private volatile boolean failNextSave = false;

    FailingLastAppliedStore(LastAppliedStore delegate) {
      this.delegate = delegate;
    }

    void failNextSave() {
      failNextSave = true;
    }

    @Override
    public void save(long lastAppliedIndex) {
      if (failNextSave) {
        failNextSave = false;
        throw new QueuePersistenceException("Simulated crash before persisting lastApplied", null);
      }
      delegate.save(lastAppliedIndex);
    }

    @Override
    public long load() {
      return delegate.load();
    }
  }

  @Test
  void normalCommittedApplicationAppendsAMessage() throws Exception {
    TopicManager topicManager = new TopicManager(tempDir.resolve("topics"));
    topicManager.createTopic("orders", 1);

    RaftQueueStateMachine stateMachine =
        new RaftQueueStateMachine(topicManager, new InMemoryLastAppliedStore());

    stateMachine.apply(
        new LogEntry(1, 1, new PublishCommand("orders", 0, "key-1", "payload-1").encode()));

    assertEquals(1, topicManager.getTopic("orders").getPartition(0).nextOffset());
    assertEquals(
        "payload-1",
        topicManager.getTopic("orders").getPartition(0).get(0).orElseThrow().payload());
  }

  @Test
  void multipleCommittedEntriesAreAppliedInOrder() throws Exception {
    TopicManager topicManager = new TopicManager(tempDir.resolve("topics"));
    topicManager.createTopic("orders", 1);

    RaftQueueStateMachine stateMachine =
        new RaftQueueStateMachine(topicManager, new InMemoryLastAppliedStore());

    stateMachine.apply(
        new LogEntry(1, 1, new PublishCommand("orders", 0, "key-1", "payload-1").encode()));
    stateMachine.apply(
        new LogEntry(1, 2, new PublishCommand("orders", 0, "key-2", "payload-2").encode()));
    stateMachine.apply(
        new LogEntry(1, 3, new PublishCommand("orders", 0, "key-3", "payload-3").encode()));

    var partition = topicManager.getTopic("orders").getPartition(0);
    assertEquals("payload-1", partition.get(0).orElseThrow().payload());
    assertEquals("payload-2", partition.get(1).orElseThrow().payload());
    assertEquals("payload-3", partition.get(2).orElseThrow().payload());
  }

  @Test
  void sameEntryIsNotAppliedTwiceWithinTheSameProcess() throws Exception {
    TopicManager topicManager = new TopicManager(tempDir.resolve("topics"));
    topicManager.createTopic("orders", 1);

    RaftQueueStateMachine stateMachine =
        new RaftQueueStateMachine(topicManager, new InMemoryLastAppliedStore());

    LogEntry entry =
        new LogEntry(1, 1, new PublishCommand("orders", 0, "key-1", "payload-1").encode());

    stateMachine.apply(entry);
    stateMachine.apply(entry); // fast-path skip via lastAppliedIndex

    assertEquals(1, topicManager.getTopic("orders").getPartition(0).nextOffset());
  }

  @Test
  void restartAfterSuccessfulApplicationDoesNotReapply() throws Exception {
    Path topicsDir = tempDir.resolve("topics");
    Path lastAppliedFile = tempDir.resolve("last-applied");

    TopicManager topicManager = new TopicManager(topicsDir);
    topicManager.createTopic("orders", 1);

    LogEntry entry =
        new LogEntry(1, 1, new PublishCommand("orders", 0, "key-1", "payload-1").encode());

    new RaftQueueStateMachine(topicManager, new FileLastAppliedStore(lastAppliedFile)).apply(entry);

    // Simulate a restart: fresh TopicManager/state machine reading the same durable files.
    TopicManager restartedTopics = new TopicManager(topicsDir);
    RaftQueueStateMachine restarted =
        new RaftQueueStateMachine(restartedTopics, new FileLastAppliedStore(lastAppliedFile));
    restarted.apply(entry);

    assertEquals(1, restartedTopics.getTopic("orders").getPartition(0).nextOffset());
  }

  @Test
  void crashBetweenWalWriteAndLastAppliedPersistDoesNotLoseOrCorruptTheApplication()
      throws Exception {
    TopicManager topicManager = new TopicManager(tempDir.resolve("topics"));
    topicManager.createTopic("orders", 1);

    FailingLastAppliedStore store = new FailingLastAppliedStore(new InMemoryLastAppliedStore());
    RaftQueueStateMachine stateMachine = new RaftQueueStateMachine(topicManager, store);

    LogEntry entry =
        new LogEntry(1, 1, new PublishCommand("orders", 0, "key-1", "payload-1").encode());

    store.failNextSave(); // the WAL write below succeeds; only the lastApplied marker fails

    assertThrows(QueuePersistenceException.class, () -> stateMachine.apply(entry));

    // The WAL write itself must have gone through before the marker write failed.
    assertEquals(1, topicManager.getTopic("orders").getPartition(0).nextOffset());
  }

  @Test
  void recoveryAfterCrashBetweenWalWriteAndLastAppliedPersistDoesNotDuplicateTheMessage()
      throws Exception {
    Path topicsDir = tempDir.resolve("topics");
    Path lastAppliedFile = tempDir.resolve("last-applied");

    TopicManager topicManager = new TopicManager(topicsDir);
    topicManager.createTopic("orders", 1);

    FailingLastAppliedStore crashingStore =
        new FailingLastAppliedStore(new FileLastAppliedStore(lastAppliedFile));
    RaftQueueStateMachine stateMachine = new RaftQueueStateMachine(topicManager, crashingStore);

    LogEntry entry =
        new LogEntry(1, 1, new PublishCommand("orders", 0, "key-1", "payload-1").encode());

    crashingStore.failNextSave();
    assertThrows(QueuePersistenceException.class, () -> stateMachine.apply(entry));

    // "Restart": lastAppliedIndex on disk is still 0 (the save never completed), so a fresh state
    // machine sees this entry as not-yet-applied by its fast-path check and must fall back to the
    // WAL-backed hasAppliedRaftIndex() check to avoid duplicating the message.
    TopicManager restartedTopics = new TopicManager(topicsDir);
    RaftQueueStateMachine restarted =
        new RaftQueueStateMachine(restartedTopics, new FileLastAppliedStore(lastAppliedFile));

    restarted.apply(entry);

    var partition = restartedTopics.getTopic("orders").getPartition(0);
    assertEquals(1, partition.nextOffset());
    assertTrue(partition.hasAppliedRaftIndex(1));
  }
}
