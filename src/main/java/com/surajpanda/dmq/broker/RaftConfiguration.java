package com.surajpanda.dmq.broker;

import com.surajpanda.dmq.queue.FileLastAppliedStore;
import com.surajpanda.dmq.queue.RaftQueueStateMachine;
import com.surajpanda.dmq.queue.RaftReplicatedQueue;
import com.surajpanda.dmq.raft.DurableRaftLog;
import com.surajpanda.dmq.raft.FileRaftLogStore;
import com.surajpanda.dmq.raft.FileRaftMetadataStore;
import com.surajpanda.dmq.raft.RaftLogApplier;
import com.surajpanda.dmq.raft.RaftLogReplicator;
import com.surajpanda.dmq.raft.RaftNode;
import com.surajpanda.dmq.topic.TopicManager;
import java.nio.file.Path;
import java.util.List;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires this broker's Raft node and the queue's Raft integration.
 *
 * <p><b>Known limitation:</b> there is no real network transport between brokers yet (no HTTP/RPC
 * client for RequestVote/AppendEntries), so this wires a single-node Raft "cluster" - this broker
 * is always its own sole voting member (clusterSize=1) regardless of how many peers are listed
 * under {@code cluster.brokers}. Every publish still genuinely goes through propose -&gt; replicate
 * -&gt; majority-commit -&gt; apply (satisfying "a message must not become committed queue state
 * merely because the leader received it"), just against a trivial one-node majority. Wiring real
 * multi-broker replication requires building that transport layer first - see the README's
 * architecture notes.
 */
@Configuration
public class RaftConfiguration {

  @Bean
  public RaftNode raftNode(BrokerProperties brokerProperties) {

    Path raftDirectory = Path.of(brokerProperties.dataDirectory()).resolve("raft");

    RaftNode node =
        new RaftNode(
            brokerProperties.id(),
            new FileRaftMetadataStore(raftDirectory.resolve("meta")),
            new DurableRaftLog(new FileRaftLogStore(raftDirectory.resolve("log"))));

    // Single-node cluster (see class javadoc): a freshly constructed node always starts
    // FOLLOWER, so it can always elect itself leader immediately - no peers to wait on.
    node.becomeCandidate();
    node.becomeLeader();

    return node;
  }

  @Bean
  public RaftLogReplicator raftLogReplicator(RaftNode raftNode) {
    RaftLogReplicator replicator = new RaftLogReplicator(raftNode, List.of(), 1);
    replicator.initializeForNewLeader();
    return replicator;
  }

  @Bean
  public RaftQueueStateMachine raftQueueStateMachine(
      TopicManager topicManager, BrokerProperties brokerProperties) {
    Path lastAppliedFile =
        Path.of(brokerProperties.dataDirectory()).resolve("raft").resolve("last-applied");
    return new RaftQueueStateMachine(topicManager, new FileLastAppliedStore(lastAppliedFile));
  }

  @Bean
  public RaftLogApplier raftLogApplier(RaftNode raftNode, RaftQueueStateMachine stateMachine) {
    return new RaftLogApplier(raftNode, stateMachine);
  }

  @Bean
  public RaftReplicatedQueue raftReplicatedQueue(
      RaftNode raftNode,
      RaftLogReplicator replicator,
      RaftLogApplier applier,
      RaftQueueStateMachine stateMachine) {
    return new RaftReplicatedQueue(raftNode, replicator, applier, stateMachine);
  }
}
