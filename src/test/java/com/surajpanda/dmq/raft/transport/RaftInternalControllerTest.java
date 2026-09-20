package com.surajpanda.dmq.raft.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.surajpanda.dmq.api.GlobalExceptionHandler;
import com.surajpanda.dmq.raft.ElectionCoordinator;
import com.surajpanda.dmq.raft.ElectionTimeout;
import com.surajpanda.dmq.raft.LogEntry;
import com.surajpanda.dmq.raft.RaftElectionDriver;
import com.surajpanda.dmq.raft.RaftLogApplier;
import com.surajpanda.dmq.raft.RaftNode;
import com.surajpanda.dmq.raft.RaftStateMachine;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

/**
 * Exercises the internal Raft HTTP surface directly (standalone MockMvc, no full Spring context) -
 * routing to RaftNode/RaftElectionDriver, request validation, and that a successful AppendEntries
 * drives this follower's own RaftLogApplier so newly committed entries are actually applied, not
 * just acknowledged.
 */
class RaftInternalControllerTest {

  private static final class RecordingStateMachine implements RaftStateMachine {
    private final List<LogEntry> applied = new ArrayList<>();

    @Override
    public void apply(LogEntry entry) {
      applied.add(entry);
    }
  }

  private static MockMvc mockMvcFor(RaftNode node, RaftStateMachine stateMachine) {
    ElectionCoordinator coordinator = new ElectionCoordinator(node, List.of(), 1);
    ElectionTimeout timeout = new ElectionTimeout(1000, 1000, new Random(1));
    RaftElectionDriver driver = new RaftElectionDriver(node, coordinator, timeout, 0);
    RaftLogApplier applier = new RaftLogApplier(node, stateMachine);
    RaftInternalController controller =
        new RaftInternalController(
            node, driver, applier, Clock.fixed(Instant.EPOCH, ZoneOffset.UTC));

    return MockMvcBuilders.standaloneSetup(controller)
        .setControllerAdvice(new GlobalExceptionHandler())
        .build();
  }

  @Test
  void requestVoteGrantsAndReturnsCurrentTerm() throws Exception {
    RaftNode node = new RaftNode("broker-1");
    MockMvc mockMvc = mockMvcFor(node, new RecordingStateMachine());

    mockMvc
        .perform(
            post("/internal/raft/request-vote")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    "{\"term\":1,\"candidateId\":\"broker-2\",\"lastLogIndex\":0,\"lastLogTerm\":0}"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.term").value(1))
        .andExpect(jsonPath("$.voteGranted").value(true));
  }

  @Test
  void requestVoteWithBlankCandidateIdIsRejected() throws Exception {
    RaftNode node = new RaftNode("broker-1");
    MockMvc mockMvc = mockMvcFor(node, new RecordingStateMachine());

    mockMvc
        .perform(
            post("/internal/raft/request-vote")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"term\":1,\"candidateId\":\"\",\"lastLogIndex\":0,\"lastLogTerm\":0}"))
        .andExpect(status().isBadRequest());
  }

  @Test
  void appendEntriesAppliesNewlyCommittedEntriesOnTheFollower() throws Exception {
    RaftNode node = new RaftNode("broker-1");
    RecordingStateMachine stateMachine = new RecordingStateMachine();
    MockMvc mockMvc = mockMvcFor(node, stateMachine);

    mockMvc
        .perform(
            post("/internal/raft/append-entries")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    "{\"term\":1,\"leaderId\":\"broker-2\",\"prevLogIndex\":0,\"prevLogTerm\":0,"
                        + "\"entries\":[{\"term\":1,\"index\":1,\"command\":\"cmd-1\"}],"
                        + "\"leaderCommit\":1}"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.success").value(true));

    assertEquals(1, stateMachine.applied.size());
    assertEquals("cmd-1", stateMachine.applied.get(0).command());
  }

  @Test
  void appendEntriesWithBlankLeaderIdIsRejected() throws Exception {
    RaftNode node = new RaftNode("broker-1");
    MockMvc mockMvc = mockMvcFor(node, new RecordingStateMachine());

    mockMvc
        .perform(
            post("/internal/raft/append-entries")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    "{\"term\":1,\"leaderId\":\"\",\"prevLogIndex\":0,\"prevLogTerm\":0,"
                        + "\"entries\":[],\"leaderCommit\":0}"))
        .andExpect(status().isBadRequest());
  }

  @Test
  void heartbeatWithBlankLeaderIdIsRejected() throws Exception {
    RaftNode node = new RaftNode("broker-1");
    MockMvc mockMvc = mockMvcFor(node, new RecordingStateMachine());

    mockMvc
        .perform(
            post("/internal/raft/heartbeat")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"term\":1,\"leaderId\":\"\"}"))
        .andExpect(status().isBadRequest());
  }

  @Test
  void heartbeatFromACurrentTermLeaderIsAccepted() throws Exception {
    RaftNode node = new RaftNode("broker-1");
    MockMvc mockMvc = mockMvcFor(node, new RecordingStateMachine());

    mockMvc
        .perform(
            post("/internal/raft/heartbeat")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"term\":0,\"leaderId\":\"broker-2\"}"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.accepted").value(true));
  }
}
