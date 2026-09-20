package com.surajpanda.dmq.queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.surajpanda.dmq.message.Message;
import com.surajpanda.dmq.raft.AppendEntriesConnection;
import com.surajpanda.dmq.raft.AppendEntriesPeer;
import com.surajpanda.dmq.raft.AppendEntriesResponse;
import com.surajpanda.dmq.raft.InProcessAppendEntriesConnection;
import com.surajpanda.dmq.raft.RaftLogApplier;
import com.surajpanda.dmq.raft.RaftLogReplicator;
import com.surajpanda.dmq.raft.RaftNode;
import com.surajpanda.dmq.topic.TopicManager;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RaftReplicatedQueueTest {

  @TempDir Path tempDir;

  private RaftReplicatedQueue singleNodeQueue(TopicManager topicManager) {
    RaftNode leader = new RaftNode("broker-1");
    leader.becomeCandidate();
    leader.becomeLeader();

    RaftLogReplicator replicator = new RaftLogReplicator(leader, List.of(), 1);
    replicator.initializeForNewLeader();

    RaftQueueStateMachine stateMachine =
        new RaftQueueStateMachine(topicManager, new InMemoryLastAppliedStore());
    RaftLogApplier applier = new RaftLogApplier(leader, stateMachine);

    return new RaftReplicatedQueue(leader, replicator, applier, stateMachine);
  }

  @Test
  void committedPublishAppearsInPartition() throws Exception {
    TopicManager topicManager = new TopicManager(tempDir);
    topicManager.createTopic("orders", 1);

    RaftReplicatedQueue queue = singleNodeQueue(topicManager);

    Message message = queue.propose(new PublishCommand("orders", 0, "key-1", "payload-1"));

    assertEquals("payload-1", message.payload());
    assertEquals(
        "payload-1",
        topicManager.getTopic("orders").getPartition(0).get(0).orElseThrow().payload());
  }

  @Test
  void uncommittedPublishNeverAppearsInPartition() throws Exception {
    TopicManager topicManager = new TopicManager(tempDir);
    topicManager.createTopic("orders", 1);

    RaftNode leader = new RaftNode("broker-1");
    leader.becomeCandidate();
    leader.becomeLeader();

    // A 3-node cluster where neither peer is reachable - majority(3)=2 can never be reached.
    AppendEntriesConnection neverAccepts =
        request -> new AppendEntriesResponse(request.term(), true, false, 0);
    List<AppendEntriesPeer> peers =
        List.of(
            new AppendEntriesPeer("broker-2", neverAccepts),
            new AppendEntriesPeer("broker-3", neverAccepts));
    RaftLogReplicator replicator = new RaftLogReplicator(leader, peers, 3);
    replicator.initializeForNewLeader();

    RaftQueueStateMachine stateMachine =
        new RaftQueueStateMachine(topicManager, new InMemoryLastAppliedStore());
    RaftLogApplier applier = new RaftLogApplier(leader, stateMachine);
    RaftReplicatedQueue queue = new RaftReplicatedQueue(leader, replicator, applier, stateMachine);

    assertThrows(
        QuorumUnavailableException.class,
        () -> queue.propose(new PublishCommand("orders", 0, "key-1", "payload-1")));

    assertTrue(topicManager.getTopic("orders").getPartition(0).get(0).isEmpty());
  }

  @Test
  void followerCannotPublish() throws Exception {
    TopicManager topicManager = new TopicManager(tempDir);
    topicManager.createTopic("orders", 1);

    RaftNode follower = new RaftNode("broker-1"); // starts FOLLOWER, never elected

    RaftLogReplicator replicator = new RaftLogReplicator(follower, List.of(), 1);
    RaftQueueStateMachine stateMachine =
        new RaftQueueStateMachine(topicManager, new InMemoryLastAppliedStore());
    RaftLogApplier applier = new RaftLogApplier(follower, stateMachine);
    RaftReplicatedQueue queue =
        new RaftReplicatedQueue(follower, replicator, applier, stateMachine);

    assertThrows(
        NotLeaderException.class,
        () -> queue.propose(new PublishCommand("orders", 0, "key-1", "payload-1")));
  }

  @Test
  void multiplePublishesAreAppliedInOrder() throws Exception {
    TopicManager topicManager = new TopicManager(tempDir);
    topicManager.createTopic("orders", 1);

    RaftReplicatedQueue queue = singleNodeQueue(topicManager);

    queue.propose(new PublishCommand("orders", 0, "key-1", "payload-1"));
    queue.propose(new PublishCommand("orders", 0, "key-2", "payload-2"));
    queue.propose(new PublishCommand("orders", 0, "key-3", "payload-3"));

    var partition = topicManager.getTopic("orders").getPartition(0);
    assertEquals("payload-1", partition.get(0).orElseThrow().payload());
    assertEquals("payload-2", partition.get(1).orElseThrow().payload());
    assertEquals("payload-3", partition.get(2).orElseThrow().payload());
  }

  @Test
  void followerAppliesTheSameCommittedCommandAsTheLeader() throws Exception {
    TopicManager leaderTopics = new TopicManager(tempDir.resolve("leader"));
    leaderTopics.createTopic("orders", 1);
    TopicManager followerTopics = new TopicManager(tempDir.resolve("follower"));
    followerTopics.createTopic("orders", 1);

    RaftNode leader = new RaftNode("broker-1");
    leader.becomeCandidate();
    leader.becomeLeader();

    RaftNode followerNode = new RaftNode("broker-2");
    RaftQueueStateMachine followerStateMachine =
        new RaftQueueStateMachine(followerTopics, new InMemoryLastAppliedStore());
    RaftLogApplier followerApplier = new RaftLogApplier(followerNode, followerStateMachine);

    AppendEntriesConnection followerConnection =
        request -> {
          AppendEntriesResponse response =
              new InProcessAppendEntriesConnection(followerNode).sendAppendEntries(request);
          followerApplier.applyCommitted();
          return response;
        };

    List<AppendEntriesPeer> peers = List.of(new AppendEntriesPeer("broker-2", followerConnection));
    RaftLogReplicator replicator = new RaftLogReplicator(leader, peers, 2);
    replicator.initializeForNewLeader();

    RaftQueueStateMachine leaderStateMachine =
        new RaftQueueStateMachine(leaderTopics, new InMemoryLastAppliedStore());
    RaftLogApplier leaderApplier = new RaftLogApplier(leader, leaderStateMachine);
    RaftReplicatedQueue queue =
        new RaftReplicatedQueue(leader, replicator, leaderApplier, leaderStateMachine);

    queue.propose(new PublishCommand("orders", 0, "key-1", "payload-1"));
    // A second round so the follower's own commitIndex catches up to what the leader committed.
    replicator.replicate(leader.getCommitIndex());

    assertEquals(
        "payload-1",
        leaderTopics.getTopic("orders").getPartition(0).get(0).orElseThrow().payload());
    assertEquals(
        "payload-1",
        followerTopics.getTopic("orders").getPartition(0).get(0).orElseThrow().payload());
  }

  @Test
  void followerConvergesAfterASingleProposeCallWithoutAnyExtraReplicationRound() throws Exception {
    TopicManager leaderTopics = new TopicManager(tempDir.resolve("leader"));
    leaderTopics.createTopic("orders", 1);
    TopicManager followerTopics = new TopicManager(tempDir.resolve("follower"));
    followerTopics.createTopic("orders", 1);

    RaftNode leader = new RaftNode("broker-1");
    leader.becomeCandidate();
    leader.becomeLeader();

    RaftNode followerNode = new RaftNode("broker-2");
    RaftQueueStateMachine followerStateMachine =
        new RaftQueueStateMachine(followerTopics, new InMemoryLastAppliedStore());
    RaftLogApplier followerApplier = new RaftLogApplier(followerNode, followerStateMachine);

    // Mirrors what the real /internal/raft/append-entries controller does: handle the request,
    // then immediately drive this follower's own applier - no test-only manual extra round.
    AppendEntriesConnection followerConnection =
        request -> {
          AppendEntriesResponse response =
              new InProcessAppendEntriesConnection(followerNode).sendAppendEntries(request);
          followerApplier.applyCommitted();
          return response;
        };

    List<AppendEntriesPeer> peers = List.of(new AppendEntriesPeer("broker-2", followerConnection));
    RaftLogReplicator replicator = new RaftLogReplicator(leader, peers, 2);
    replicator.initializeForNewLeader();

    RaftQueueStateMachine leaderStateMachine =
        new RaftQueueStateMachine(leaderTopics, new InMemoryLastAppliedStore());
    RaftLogApplier leaderApplier = new RaftLogApplier(leader, leaderStateMachine);
    RaftReplicatedQueue queue =
        new RaftReplicatedQueue(leader, replicator, leaderApplier, leaderStateMachine);

    queue.propose(new PublishCommand("orders", 0, "key-1", "payload-1"));

    // No extra manual replicate() round here - propose() alone must be enough for the follower to
    // learn the commitIndex and apply the entry.
    assertEquals(
        "payload-1",
        followerTopics.getTopic("orders").getPartition(0).get(0).orElseThrow().payload());
  }

  @Test
  void concurrentProposeAndStepDownNeverLeaksARawIllegalStateException() throws Exception {
    // A regression guard for a real race found via the multi-broker integration tests: propose()'s
    // own leader check and replicator.replicate() are not atomic with each other, so this node can
    // step down (a concurrent higher-term AppendEntries/RequestVote handled on another thread, or -
    // in production - the periodic scheduler's own replicate() call) in the narrow window between
    // them. replicate() then throws IllegalStateException; propose() must translate that into
    // NotLeaderException rather than let a raw IllegalStateException escape to the caller.
    TopicManager topicManager = new TopicManager(tempDir);
    topicManager.createTopic("orders", 1);

    RaftNode leader = new RaftNode("broker-1");
    leader.becomeCandidate();
    leader.becomeLeader();

    RaftNode follower = new RaftNode("broker-2");
    List<AppendEntriesPeer> peers =
        List.of(new AppendEntriesPeer("broker-2", new InProcessAppendEntriesConnection(follower)));
    RaftLogReplicator replicator = new RaftLogReplicator(leader, peers, 2);
    replicator.initializeForNewLeader();

    RaftQueueStateMachine stateMachine =
        new RaftQueueStateMachine(topicManager, new InMemoryLastAppliedStore());
    RaftLogApplier applier = new RaftLogApplier(leader, stateMachine);
    RaftReplicatedQueue queue = new RaftReplicatedQueue(leader, replicator, applier, stateMachine);

    ExecutorService executor = Executors.newFixedThreadPool(4);
    AtomicInteger keyCounter = new AtomicInteger();
    List<Future<?>> futures = new ArrayList<>();

    try {
      for (int i = 0; i < 200; i++) {
        futures.add(
            executor.submit(
                () -> {
                  try {
                    queue.propose(
                        new PublishCommand(
                            "orders", 0, "key-" + keyCounter.incrementAndGet(), "payload"));
                  } catch (NotLeaderException | QuorumUnavailableException expected) {
                    // Both are acceptable outcomes once this node steps down mid-run - only a raw
                    // IllegalStateException (or any other unexpected exception) is the bug.
                  }
                }));
      }

      for (int i = 0; i < 10; i++) {
        futures.add(executor.submit(() -> leader.advanceTerm(leader.getCurrentTerm() + 1)));
      }

      for (Future<?> future : futures) {
        future.get(15, TimeUnit.SECONDS);
      }
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void restartingDoesNotDuplicateAnAlreadyAppliedCommand() throws Exception {
    Path metaDir = tempDir.resolve("node");
    TopicManager topicManager = new TopicManager(tempDir.resolve("topics"));
    topicManager.createTopic("orders", 1);

    var lastAppliedStore = new FileLastAppliedStore(metaDir.resolve("last-applied"));

    RaftNode leader = new RaftNode("broker-1");
    leader.becomeCandidate();
    leader.becomeLeader();
    RaftLogReplicator replicator = new RaftLogReplicator(leader, List.of(), 1);
    replicator.initializeForNewLeader();
    RaftQueueStateMachine stateMachine = new RaftQueueStateMachine(topicManager, lastAppliedStore);
    RaftLogApplier applier = new RaftLogApplier(leader, stateMachine);
    RaftReplicatedQueue queue = new RaftReplicatedQueue(leader, replicator, applier, stateMachine);

    queue.propose(new PublishCommand("orders", 0, "key-1", "payload-1"));

    // Simulate a restart: a fresh state machine reading the same durable last-applied index, then
    // re-applying the already-committed entry (as would happen if commitIndex is re-derived).
    RaftQueueStateMachine restartedStateMachine =
        new RaftQueueStateMachine(
            topicManager, new FileLastAppliedStore(metaDir.resolve("last-applied")));
    restartedStateMachine.apply(leader.getLog().get(1).orElseThrow());

    assertEquals(1, topicManager.getTopic("orders").getPartition(0).nextOffset());
  }

  @Test
  void singleNodeClusterCanPublishWithoutAnyPeers() throws Exception {
    TopicManager topicManager = new TopicManager(tempDir);
    topicManager.createTopic("orders", 1);

    RaftReplicatedQueue queue = singleNodeQueue(topicManager);

    Message message = queue.propose(new PublishCommand("orders", 0, "key-1", "payload-1"));

    assertEquals("payload-1", message.payload());
  }
}
