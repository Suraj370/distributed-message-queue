package com.surajpanda.dmq.raft;

/**
 * In-process stand-in for a real RPC transport, mirroring InProcessRaftPeerConnection and
 * InProcessHeartbeatConnection but for the AppendEntries channel. Delegates directly to the peer's
 * RaftNode so log-replication behavior can be exercised deterministically without networking.
 */
public class InProcessAppendEntriesConnection implements AppendEntriesConnection {

  private final RaftNode peerNode;

  public InProcessAppendEntriesConnection(RaftNode peerNode) {
    this.peerNode = peerNode;
  }

  @Override
  public AppendEntriesResponse sendAppendEntries(AppendEntriesRequest request) {
    return peerNode.handleAppendEntries(request);
  }
}
