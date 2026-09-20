package com.surajpanda.dmq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.surajpanda.dmq.consumer.Consumer;
import com.surajpanda.dmq.consumer.ConsumerGroup;
import com.surajpanda.dmq.consumer.FileOffsetStore;
import com.surajpanda.dmq.message.Message;
import com.surajpanda.dmq.partition.Partition;
import com.surajpanda.dmq.queue.PublishCommand;
import com.surajpanda.dmq.queue.QuorumUnavailableException;
import com.surajpanda.dmq.raft.FileRaftLogStore;
import com.surajpanda.dmq.raft.RaftPersistenceException;
import com.surajpanda.dmq.raft.RaftState;
import com.surajpanda.dmq.support.SimulatedCluster;
import com.surajpanda.dmq.wal.Wal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end, deterministic distributed-system tests proving the acceptance criteria: committed
 * commands mutate queue state, uncommitted ones never do, and the system behaves correctly across
 * leader/follower failure, quorum loss, and restart. All in-process via SimulatedCluster - no real
 * networking, no sleeping, no randomness.
 */
class DistributedFailureTest {

  @TempDir Path tempDir;

  // TEST 1: leader publishes, replicates, majority commits, all healthy nodes eventually apply.
  @Test
  void healthyThreeNodeClusterConvergesOnACommittedPublish() throws Exception {
    SimulatedCluster cluster = new SimulatedCluster(3, tempDir, "orders", 1);
    cluster.electLeader(0);

    Message published =
        cluster.node(0).queue().propose(new PublishCommand("orders", 0, "key-1", "payload-1"));
    cluster.replicateAndCommit(0);

    assertEquals("payload-1", published.payload());
    for (int i = 0; i < 3; i++) {
      assertEquals(
          "payload-1",
          cluster
              .node(i)
              .topicManager()
              .getTopic("orders")
              .getPartition(0)
              .get(0)
              .orElseThrow()
              .payload());
    }
  }

  // TEST 2: leader + one follower available; publish succeeds and commits.
  @Test
  void leaderPlusOneFollowerCanCommit() throws Exception {
    SimulatedCluster cluster = new SimulatedCluster(3, tempDir, "orders", 1);
    cluster.electLeader(0);
    cluster.takeDown(2);

    cluster.node(0).queue().propose(new PublishCommand("orders", 0, "key-1", "payload-1"));
    cluster.replicateAndCommit(0);

    assertEquals(1, cluster.node(0).raftNode().getCommitIndex());
    assertEquals(
        "payload-1",
        cluster
            .node(1)
            .topicManager()
            .getTopic("orders")
            .getPartition(0)
            .get(0)
            .orElseThrow()
            .payload());
  }

  // TEST 3: only the leader available; publish must NOT commit.
  @Test
  void onlyLeaderAvailableCannotCommit() throws Exception {
    SimulatedCluster cluster = new SimulatedCluster(3, tempDir, "orders", 1);
    cluster.electLeader(0);
    cluster.takeDown(1);
    cluster.takeDown(2);

    assertThrows(
        QuorumUnavailableException.class,
        () ->
            cluster.node(0).queue().propose(new PublishCommand("orders", 0, "key-1", "payload-1")));

    assertEquals(0, cluster.node(0).raftNode().getCommitIndex());
    assertTrue(cluster.node(0).topicManager().getTopic("orders").getPartition(0).get(0).isEmpty());
  }

  // TEST 4: leader fails after a commit; a surviving node is elected; the committed message
  // remains available.
  @Test
  void committedMessageSurvivesLeaderFailureAndReelection() throws Exception {
    SimulatedCluster cluster = new SimulatedCluster(3, tempDir, "orders", 1);
    cluster.electLeader(0);
    cluster.node(0).queue().propose(new PublishCommand("orders", 0, "key-1", "payload-1"));
    cluster.replicateAndCommit(0);

    cluster.takeDown(0); // leader fails

    cluster.electLeader(1); // node-1 wins with node-1(self) + node-2

    assertEquals(RaftState.LEADER, cluster.node(1).raftNode().getState());
    assertEquals(
        "payload-1",
        cluster
            .node(1)
            .topicManager()
            .getTopic("orders")
            .getPartition(0)
            .get(0)
            .orElseThrow()
            .payload());
  }

  // TEST 5: leader fails with an uncommitted entry - it must not be treated as committed just
  // because it existed on the old leader.
  @Test
  void uncommittedEntryOnFailedLeaderIsNotTreatedAsCommitted() throws Exception {
    SimulatedCluster cluster = new SimulatedCluster(3, tempDir, "orders", 1);
    cluster.electLeader(0);
    cluster.takeDown(1);
    cluster.takeDown(2);

    // Append directly to the leader's own log without a majority ever seeing it (propose() would
    // throw, so we drive the log append the same way propose() does, just without asserting).
    cluster
        .node(0)
        .raftNode()
        .getLog()
        .appendCommand(cluster.node(0).raftNode().getCurrentTerm(), "unused");

    cluster.takeDown(0); // the old leader (with the uncommitted entry) is now gone
    cluster.setLinkAvailable(1, 2, true); // only reconnect the two survivors to each other

    cluster.electLeader(1);

    // node-1 never received that entry, so it is simply absent from the new leader's log/state -
    // not "uncommitted but present", just gone, exactly as Raft requires.
    assertEquals(0, cluster.node(1).raftNode().getLog().lastIndex());
    assertEquals(0, cluster.node(1).raftNode().getCommitIndex());
  }

  // TEST 6: a follower goes down; the leader continues operating while quorum remains.
  @Test
  void leaderContinuesOperatingWhileQuorumRemainsAfterAFollowerFailure() throws Exception {
    SimulatedCluster cluster = new SimulatedCluster(3, tempDir, "orders", 1);
    cluster.electLeader(0);

    cluster.takeDown(2); // one follower down, quorum (leader + node-1) still available

    cluster.node(0).queue().propose(new PublishCommand("orders", 0, "key-1", "payload-1"));
    cluster.replicateAndCommit(0);

    assertEquals(1, cluster.node(0).raftNode().getCommitIndex());
  }

  // TEST 7: quorum is lost; new writes must not become committed.
  @Test
  void quorumLossPreventsNewCommits() throws Exception {
    SimulatedCluster cluster = new SimulatedCluster(3, tempDir, "orders", 1);
    cluster.electLeader(0);
    cluster.takeDown(1);
    cluster.takeDown(2);

    assertThrows(
        QuorumUnavailableException.class,
        () ->
            cluster.node(0).queue().propose(new PublishCommand("orders", 0, "key-1", "payload-1")));

    assertEquals(0, cluster.node(0).raftNode().getCommitIndex());
  }

  // TEST 8: a follower restarts and catches up with the leader's committed log/state.
  @Test
  void restartedFollowerCatchesUpWithLeader() throws Exception {
    SimulatedCluster cluster = new SimulatedCluster(3, tempDir, "orders", 1);
    cluster.electLeader(0);

    cluster.takeDown(1); // this follower never sees the entry before it goes down
    cluster.node(0).queue().propose(new PublishCommand("orders", 0, "key-1", "payload-1"));
    cluster.replicateAndCommit(0); // committed via node-0 + node-2

    cluster.restartNode(1); // simulates the follower crashing and restarting
    cluster.bringUp(1); // and rejoining the cluster

    // The leader replicates again; the restarted follower (empty log, having never seen the
    // entry) must catch up via the normal backtracking AppendEntries path.
    cluster.replicateAndCommit(0);

    assertEquals(1, cluster.node(1).raftNode().getLog().lastIndex());
    assertEquals(
        "payload-1",
        cluster
            .node(1)
            .topicManager()
            .getTopic("orders")
            .getPartition(0)
            .get(0)
            .orElseThrow()
            .payload());
  }

  // TEST 9: consumer processes a message but crashes before committing its offset - redelivered.
  @Test
  void consumerCrashBeforeOffsetCommitCausesRedelivery() throws Exception {
    Partition partition = new Partition("orders", 0, new Wal(tempDir.resolve("p0.log")));
    partition.append("key-1", "message-1");

    ConsumerGroup group =
        new ConsumerGroup("orders-group", new FileOffsetStore(tempDir.resolve("offsets")));
    Consumer consumer = new Consumer(partition);

    Message processedButNotCommitted = consumer.fetchNext(group).orElseThrow();
    // crash happens here, before commitOffset()

    Message redelivered = consumer.fetchNext(group).orElseThrow();

    assertEquals(processedButNotCommitted.offset(), redelivered.offset());
  }

  // TEST 10: consumer processes and commits the offset - a restart/rebalance must not redeliver.
  @Test
  void committedOffsetPreventsRedeliveryAfterRestart() throws Exception {
    Partition partition = new Partition("orders", 0, new Wal(tempDir.resolve("p0.log")));
    partition.append("key-1", "message-1");
    partition.append("key-2", "message-2");
    Path offsetsDir = tempDir.resolve("offsets");

    ConsumerGroup before = new ConsumerGroup("orders-group", new FileOffsetStore(offsetsDir));
    Consumer beforeConsumer = new Consumer(partition);
    Message first = beforeConsumer.fetchNext(before).orElseThrow();
    before.commitOffset(partition, first.offset() + 1);

    // Simulate a restart/rebalance: a brand new ConsumerGroup instance, same durable offsets.
    ConsumerGroup after = new ConsumerGroup("orders-group", new FileOffsetStore(offsetsDir));
    Consumer afterConsumer = new Consumer(partition);
    Message next = afterConsumer.fetchNext(after).orElseThrow();

    assertEquals("message-2", next.payload());
  }

  // TEST 11: a Raft node restarts; term/vote/log recover correctly.
  @Test
  void raftNodeRestartRecoversTermVoteAndLog() throws Exception {
    SimulatedCluster cluster = new SimulatedCluster(3, tempDir, "orders", 1);
    cluster.electLeader(0);
    cluster.node(0).queue().propose(new PublishCommand("orders", 0, "key-1", "payload-1"));
    cluster.replicateAndCommit(0);

    long termBeforeRestart = cluster.node(1).raftNode().getCurrentTerm();
    long lastIndexBeforeRestart = cluster.node(1).raftNode().getLog().lastIndex();

    cluster.restartNode(1);

    assertEquals(termBeforeRestart, cluster.node(1).raftNode().getCurrentTerm());
    assertEquals(RaftState.FOLLOWER, cluster.node(1).raftNode().getState());
    assertEquals(lastIndexBeforeRestart, cluster.node(1).raftNode().getLog().lastIndex());
    assertEquals(
        "payload-1",
        PublishCommand.decode(cluster.node(1).raftNode().getLog().get(1).orElseThrow().command())
            .payload());
  }

  // TEST 12: the Raft log store now persists every mutation as a complete, atomically-promoted
  // snapshot (temp file, fully written and forced, then renamed over the real file) rather than an
  // append-only stream of incremental records - see FileRaftLogStore. That means the real file is
  // never partially written by this code's own operation, so a corrupted line appearing in it is
  // always genuine corruption, not a benign crash-mid-write artifact, and restart must fail loudly
  // instead of silently discarding it.
  @Test
  void corruptedRealRaftLogFileFailsRestartLoudly() throws Exception {
    SimulatedCluster cluster = new SimulatedCluster(3, tempDir, "orders", 1);
    cluster.electLeader(0);
    cluster.node(0).queue().propose(new PublishCommand("orders", 0, "key-1", "payload-1"));
    cluster.replicateAndCommit(0);

    Path raftLogFile = tempDir.resolve(cluster.id(0)).resolve("raft-log");
    Files.writeString(raftLogFile, "E|2|1|not-complete", StandardOpenOption.APPEND);

    assertThrows(RaftPersistenceException.class, () -> cluster.restartNode(0));
  }

  // The realistic crash-mid-write artifact under the new atomic-replacement design is an
  // incomplete ".tmp" file left behind by an interrupted write that never reached the atomic
  // rename - not a corrupted real file. That must be harmless on restart.
  @Test
  void strayIncompleteRaftLogTempFileDoesNotAffectRestart() throws Exception {
    SimulatedCluster cluster = new SimulatedCluster(3, tempDir, "orders", 1);
    cluster.electLeader(0);
    cluster.node(0).queue().propose(new PublishCommand("orders", 0, "key-1", "payload-1"));
    cluster.replicateAndCommit(0);

    Path raftLogFile = tempDir.resolve(cluster.id(0)).resolve("raft-log");
    Files.writeString(raftLogFile.resolveSibling("raft-log.tmp"), "E|2|1|not-complete");

    cluster.restartNode(0);

    assertEquals(1, cluster.node(0).raftNode().getLog().lastIndex());
    assertEquals(
        "payload-1",
        PublishCommand.decode(cluster.node(0).raftNode().getLog().get(1).orElseThrow().command())
            .payload());
    assertEquals(1, new FileRaftLogStore(raftLogFile).loadAll().size());
  }
}
