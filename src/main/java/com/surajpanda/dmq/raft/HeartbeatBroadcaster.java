package com.surajpanda.dmq.raft;

import java.util.List;

/**
 * Sends one round of heartbeats from a LEADER RaftNode to its peers. Synchronous and one-shot,
 * mirroring ElectionCoordinator's single-attempt shape; a scheduler is responsible for calling
 * broadcastIfLeader() repeatedly. Carries no log entries - this is a liveness signal only.
 */
public class HeartbeatBroadcaster {

  private final RaftNode localNode;
  private final List<HeartbeatPeer> peers;

  public HeartbeatBroadcaster(RaftNode localNode, List<HeartbeatPeer> peers) {
    this.localNode = localNode;
    this.peers = List.copyOf(peers);
  }

  public void broadcast() {

    if (localNode.getState() != RaftState.LEADER) {
      throw new IllegalStateException(
          "Only a LEADER sends heartbeats, was: " + localNode.getState());
    }

    long term = localNode.getCurrentTerm();

    for (HeartbeatPeer peer : peers) {

      if (localNode.getState() != RaftState.LEADER) {
        break;
      }

      HeartbeatResponse response =
          peer.connection().sendHeartbeat(new Heartbeat(term, localNode.getNodeId()));

      if (response.term() > localNode.getCurrentTerm()) {
        localNode.advanceTerm(response.term());
      }
    }
  }

  public void broadcastIfLeader() {
    if (localNode.getState() == RaftState.LEADER) {
      broadcast();
    }
  }
}
