package com.surajpanda.dmq.raft.support;

import com.surajpanda.dmq.raft.RaftPeerConnection;
import com.surajpanda.dmq.raft.RequestVoteRequest;
import com.surajpanda.dmq.raft.RequestVoteResponse;

/**
 * Deterministic, controllable failure injection for the RequestVote channel: wraps a real
 * connection and can be toggled offline to simulate a crashed/partitioned peer. While offline,
 * every request is answered with a non-granting response (the same observable effect a genuinely
 * unreachable peer has on an election's vote tally) rather than throwing, so tests stay
 * deterministic and don't need to handle unexpected exceptions from the orchestration classes.
 */
public class FaultyRaftPeerConnection implements RaftPeerConnection {

  private final RaftPeerConnection delegate;
  private volatile boolean available = true;

  public FaultyRaftPeerConnection(RaftPeerConnection delegate) {
    this.delegate = delegate;
  }

  public void setAvailable(boolean available) {
    this.available = available;
  }

  @Override
  public RequestVoteResponse requestVote(RequestVoteRequest request) {
    if (!available) {
      return new RequestVoteResponse(request.term(), false);
    }
    return delegate.requestVote(request);
  }
}
