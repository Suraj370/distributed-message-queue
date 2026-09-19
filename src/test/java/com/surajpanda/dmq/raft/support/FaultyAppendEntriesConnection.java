package com.surajpanda.dmq.raft.support;

import com.surajpanda.dmq.raft.AppendEntriesConnection;
import com.surajpanda.dmq.raft.AppendEntriesRequest;
import com.surajpanda.dmq.raft.AppendEntriesResponse;

/**
 * Deterministic, controllable failure injection for the AppendEntries channel: wraps a real
 * connection and can be toggled offline to simulate a crashed/partitioned follower or a dropped
 * replication RPC. While offline, requests are answered with a non-accepting, non-progressing
 * response rather than thrown exceptions, keeping the leader's replication loop deterministic.
 */
public class FaultyAppendEntriesConnection implements AppendEntriesConnection {

  private final AppendEntriesConnection delegate;
  private volatile boolean available = true;

  public FaultyAppendEntriesConnection(AppendEntriesConnection delegate) {
    this.delegate = delegate;
  }

  public void setAvailable(boolean available) {
    this.available = available;
  }

  @Override
  public AppendEntriesResponse sendAppendEntries(AppendEntriesRequest request) {
    if (!available) {
      return new AppendEntriesResponse(request.term(), true, false, 0);
    }
    return delegate.sendAppendEntries(request);
  }
}
