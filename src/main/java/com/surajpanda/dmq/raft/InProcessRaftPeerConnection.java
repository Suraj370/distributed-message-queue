package com.surajpanda.dmq.raft;

/**
 * In-process stand-in for a real RPC transport, mirroring
 * com.surajpanda.dmq.replication.InProcessReplicaConnection. Delegates directly to the peer's
 * RaftNode so election logic can be exercised deterministically without networking.
 */
public class InProcessRaftPeerConnection implements RaftPeerConnection {

  private final RaftNode peerNode;

  public InProcessRaftPeerConnection(RaftNode peerNode) {
    this.peerNode = peerNode;
  }

  @Override
  public RequestVoteResponse requestVote(RequestVoteRequest request) {
    return peerNode.handleRequestVote(request);
  }
}
