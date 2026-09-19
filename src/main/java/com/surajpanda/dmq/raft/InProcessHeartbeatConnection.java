package com.surajpanda.dmq.raft;

/**
 * In-process stand-in for a real RPC transport, mirroring InProcessRaftPeerConnection but for the
 * heartbeat channel. Delegates directly to the peer's RaftNode so heartbeat behavior can be
 * exercised deterministically without networking.
 */
public class InProcessHeartbeatConnection implements HeartbeatConnection {

  private final RaftNode peerNode;

  public InProcessHeartbeatConnection(RaftNode peerNode) {
    this.peerNode = peerNode;
  }

  @Override
  public HeartbeatResponse sendHeartbeat(Heartbeat heartbeat) {
    return peerNode.handleHeartbeat(heartbeat);
  }
}
