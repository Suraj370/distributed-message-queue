package com.surajpanda.dmq.api;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.surajpanda.dmq.broker.Broker;
import com.surajpanda.dmq.broker.BrokerProperties;
import com.surajpanda.dmq.consumer.ConsumerGroupRegistry;
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
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

/** Exercises the read-only message/consumer HTTP surface added for the Testcontainers milestone. */
class ConsumerAndMessageReadControllerTest {

  @TempDir Path tempDir;

  private MockMvc mockMvc() throws Exception {
    TopicManager topicManager = new TopicManager(tempDir.resolve("topics"));
    topicManager.createTopic("orders", 1);

    RaftNode leader = new RaftNode("broker-1");
    leader.becomeCandidate();
    leader.becomeLeader();
    RaftLogReplicator replicator = new RaftLogReplicator(leader, List.of(), 1);
    replicator.initializeForNewLeader();
    RaftQueueStateMachine stateMachine =
        new RaftQueueStateMachine(topicManager, new InMemoryLastAppliedStore());
    RaftLogApplier applier = new RaftLogApplier(leader, stateMachine);
    RaftReplicatedQueue queue = new RaftReplicatedQueue(leader, replicator, applier, stateMachine);

    BrokerProperties brokerProperties =
        new BrokerProperties("broker-1", "localhost", 8080, tempDir.resolve("data").toString());
    Broker broker = new Broker(topicManager, brokerProperties, queue);

    ConsumerGroupRegistry registry = new ConsumerGroupRegistry(brokerProperties);

    return MockMvcBuilders.standaloneSetup(
            new MessageController(broker), new ConsumerController(topicManager, registry))
        .build();
  }

  @Test
  void readReturnsThePublishedMessageAtItsOffset() throws Exception {
    MockMvc mockMvc = mockMvc();

    mockMvc.perform(
        post("/api/v1/messages")
            .param("topic", "orders")
            .param("key", "k1")
            .param("payload", "v1"));

    mockMvc
        .perform(
            org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get(
                    "/api/v1/messages")
                .param("topic", "orders")
                .param("partition", "0")
                .param("offset", "0"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.payload").value("v1"));
  }

  @Test
  void readReturns404ForAnOffsetNotYetPresent() throws Exception {
    MockMvc mockMvc = mockMvc();

    mockMvc
        .perform(
            org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get(
                    "/api/v1/messages")
                .param("topic", "orders")
                .param("partition", "0")
                .param("offset", "0"))
        .andExpect(status().isNotFound());
  }

  @Test
  void fetchThenCommitPreventsRedeliveryOfTheSameMessage() throws Exception {
    MockMvc mockMvc = mockMvc();

    mockMvc.perform(
        post("/api/v1/messages")
            .param("topic", "orders")
            .param("key", "k1")
            .param("payload", "v1"));
    mockMvc.perform(
        post("/api/v1/messages")
            .param("topic", "orders")
            .param("key", "k1")
            .param("payload", "v2"));

    mockMvc
        .perform(
            post("/api/v1/consumer-groups/g1/fetch")
                .param("topic", "orders")
                .param("partition", "0"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.payload").value("v1"))
        .andExpect(jsonPath("$.offset").value(0));

    mockMvc
        .perform(
            post("/api/v1/consumer-groups/g1/commit")
                .param("topic", "orders")
                .param("partition", "0")
                .param("offset", "1"))
        .andExpect(status().isOk());

    mockMvc
        .perform(
            post("/api/v1/consumer-groups/g1/fetch")
                .param("topic", "orders")
                .param("partition", "0"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.payload").value("v2"))
        .andExpect(jsonPath("$.offset").value(1));
  }

  @Test
  void fetchWithoutCommitRedeliversTheSameMessage() throws Exception {
    MockMvc mockMvc = mockMvc();

    mockMvc.perform(
        post("/api/v1/messages")
            .param("topic", "orders")
            .param("key", "k1")
            .param("payload", "v1"));

    mockMvc
        .perform(
            post("/api/v1/consumer-groups/g2/fetch")
                .param("topic", "orders")
                .param("partition", "0"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.offset").value(0));

    // No commit in between - the same message must be redelivered.
    mockMvc
        .perform(
            post("/api/v1/consumer-groups/g2/fetch")
                .param("topic", "orders")
                .param("partition", "0"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.offset").value(0))
        .andExpect(jsonPath("$.payload").value("v1"));
  }

  @Test
  void fetchReturnsNoContentWhenGroupIsCaughtUp() throws Exception {
    MockMvc mockMvc = mockMvc();

    mockMvc
        .perform(
            post("/api/v1/consumer-groups/g3/fetch")
                .param("topic", "orders")
                .param("partition", "0"))
        .andExpect(status().isNoContent());
  }
}
