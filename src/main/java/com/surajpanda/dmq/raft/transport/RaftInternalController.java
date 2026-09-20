package com.surajpanda.dmq.raft.transport;

import com.surajpanda.dmq.raft.AppendEntriesRequest;
import com.surajpanda.dmq.raft.AppendEntriesResponse;
import com.surajpanda.dmq.raft.Heartbeat;
import com.surajpanda.dmq.raft.HeartbeatResponse;
import com.surajpanda.dmq.raft.RaftElectionDriver;
import com.surajpanda.dmq.raft.RaftLogApplier;
import com.surajpanda.dmq.raft.RaftNode;
import com.surajpanda.dmq.raft.RequestVoteRequest;
import com.surajpanda.dmq.raft.RequestVoteResponse;
import java.time.Clock;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Internal broker-to-broker Raft RPC surface - never a public message API, deliberately kept under
 * its own path so it is obvious this is transport plumbing, not client-facing functionality.
 *
 * <p>AppendEntries and Heartbeat are routed through RaftElectionDriver rather than straight to
 * RaftNode, so a request from a current/newer-term leader resets this follower's own election
 * timeout exactly as it would in the in-process/simulated wiring - a follower that never resets its
 * timeout would keep starting spurious elections against a perfectly healthy leader. RequestVote
 * goes straight to RaftNode, matching every existing in-process/simulated connection, which never
 * resets the timeout on a vote grant either.
 *
 * <p>After a successful AppendEntries, this broker's own commitIndex may have advanced, so the
 * local RaftLogApplier is driven right here - the leader's applier is only ever driven from its own
 * propose() call, so nothing else would apply newly committed entries on a follower.
 */
@RestController
@RequestMapping("/internal/raft")
public class RaftInternalController {

  private final RaftNode raftNode;
  private final RaftElectionDriver electionDriver;
  private final RaftLogApplier logApplier;
  private final Clock clock;

  public RaftInternalController(
      RaftNode raftNode,
      RaftElectionDriver electionDriver,
      RaftLogApplier logApplier,
      Clock clock) {
    this.raftNode = raftNode;
    this.electionDriver = electionDriver;
    this.logApplier = logApplier;
    this.clock = clock;
  }

  @PostMapping("/request-vote")
  public ResponseEntity<RequestVoteResponse> requestVote(@RequestBody RequestVoteRequest request) {
    validate(request);
    return ResponseEntity.ok(raftNode.handleRequestVote(request));
  }

  @PostMapping("/append-entries")
  public ResponseEntity<AppendEntriesResponse> appendEntries(
      @RequestBody AppendEntriesRequest request) {
    validate(request);
    AppendEntriesResponse response =
        electionDriver.onAppendEntriesReceived(request, clock.millis());
    logApplier.applyCommitted();
    return ResponseEntity.ok(response);
  }

  @PostMapping("/heartbeat")
  public ResponseEntity<HeartbeatResponse> heartbeat(@RequestBody Heartbeat heartbeat) {
    validate(heartbeat);
    return ResponseEntity.ok(electionDriver.onHeartbeatReceived(heartbeat, clock.millis()));
  }

  private static void validate(RequestVoteRequest request) {
    if (request.candidateId() == null || request.candidateId().isBlank()) {
      throw new IllegalArgumentException("candidateId must not be blank");
    }
  }

  private static void validate(AppendEntriesRequest request) {
    if (request.leaderId() == null || request.leaderId().isBlank()) {
      throw new IllegalArgumentException("leaderId must not be blank");
    }
  }

  private static void validate(Heartbeat heartbeat) {
    if (heartbeat.leaderId() == null || heartbeat.leaderId().isBlank()) {
      throw new IllegalArgumentException("leaderId must not be blank");
    }
  }
}
