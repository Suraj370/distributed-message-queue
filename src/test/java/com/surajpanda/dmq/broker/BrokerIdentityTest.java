package com.surajpanda.dmq.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.surajpanda.dmq.queue.InMemoryLastAppliedStore;
import com.surajpanda.dmq.queue.RaftQueueStateMachine;
import com.surajpanda.dmq.queue.RaftReplicatedQueue;
import com.surajpanda.dmq.raft.RaftLogApplier;
import com.surajpanda.dmq.raft.RaftLogReplicator;
import com.surajpanda.dmq.raft.RaftNode;
import com.surajpanda.dmq.topic.TopicManager;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BrokerIdentityTest {

  @TempDir Path tempDir;

  @Test
  void shouldExposeItsOwnBrokerId() throws Exception {

    TopicManager topicManager = new TopicManager(tempDir);

    BrokerProperties brokerProperties =
        new BrokerProperties("broker-1", "localhost", 8080, tempDir.toString());

    RaftNode raftNode = new RaftNode(brokerProperties.id());
    raftNode.becomeCandidate();
    raftNode.becomeLeader();
    RaftLogReplicator replicator = new RaftLogReplicator(raftNode, List.of(), 1);
    replicator.initializeForNewLeader();
    RaftQueueStateMachine stateMachine =
        new RaftQueueStateMachine(topicManager, new InMemoryLastAppliedStore());
    RaftReplicatedQueue replicatedQueue =
        new RaftReplicatedQueue(
            raftNode, replicator, new RaftLogApplier(raftNode, stateMachine), stateMachine);

    Broker broker = new Broker(topicManager, brokerProperties, replicatedQueue);

    assertEquals("broker-1", broker.getBrokerId());
  }
}
