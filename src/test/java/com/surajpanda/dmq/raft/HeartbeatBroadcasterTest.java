package com.surajpanda.dmq.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;

class HeartbeatBroadcasterTest {

  @Test
  void shouldRejectBroadcastingWhenNotLeader() {
    RaftNode node = new RaftNode("broker-1");
    HeartbeatBroadcaster broadcaster = new HeartbeatBroadcaster(node, List.of());

    assertThrows(IllegalStateException.class, broadcaster::broadcast);
  }

  @Test
  void shouldDeliverHeartbeatToMultipleFollowers() {
    RaftNode leader = new RaftNode("broker-1");
    leader.becomeCandidate();
    leader.becomeLeader();

    RaftNode follower1 = new RaftNode("broker-2");
    RaftNode follower2 = new RaftNode("broker-3");

    List<HeartbeatPeer> peers =
        List.of(
            new HeartbeatPeer("broker-2", new InProcessHeartbeatConnection(follower1)),
            new HeartbeatPeer("broker-3", new InProcessHeartbeatConnection(follower2)));

    new HeartbeatBroadcaster(leader, peers).broadcast();

    assertEquals(1, follower1.getCurrentTerm());
    assertEquals(RaftState.FOLLOWER, follower1.getState());
    assertEquals(1, follower2.getCurrentTerm());
    assertEquals(RaftState.FOLLOWER, follower2.getState());
  }

  @Test
  void shouldSendANewRoundEachTimeBroadcastIsCalled() {
    RaftNode leader = new RaftNode("broker-1");
    leader.becomeCandidate();
    leader.becomeLeader();

    RaftNode follower = new RaftNode("broker-2");
    int[] heartbeatsReceived = {0};

    HeartbeatConnection countingConnection =
        heartbeat -> {
          heartbeatsReceived[0]++;
          return follower.handleHeartbeat(heartbeat);
        };

    HeartbeatBroadcaster broadcaster =
        new HeartbeatBroadcaster(
            leader, List.of(new HeartbeatPeer("broker-2", countingConnection)));

    broadcaster.broadcast();
    broadcaster.broadcast();
    broadcaster.broadcast();

    assertEquals(3, heartbeatsReceived[0]);
  }

  @Test
  void shouldStepDownWhenAPeerRespondsWithANewerTerm() {
    RaftNode leader = new RaftNode("broker-1");
    leader.becomeCandidate();
    leader.becomeLeader();

    RaftNode aheadPeer = new RaftNode("broker-2");
    aheadPeer.advanceTerm(9);

    List<HeartbeatPeer> peers =
        List.of(new HeartbeatPeer("broker-2", new InProcessHeartbeatConnection(aheadPeer)));

    new HeartbeatBroadcaster(leader, peers).broadcast();

    assertEquals(RaftState.FOLLOWER, leader.getState());
    assertEquals(9, leader.getCurrentTerm());
  }

  @Test
  void broadcastIfLeaderShouldDoNothingWhenNotLeader() {
    RaftNode node = new RaftNode("broker-1");
    RaftNode follower = new RaftNode("broker-2");

    HeartbeatBroadcaster broadcaster =
        new HeartbeatBroadcaster(
            node,
            List.of(new HeartbeatPeer("broker-2", new InProcessHeartbeatConnection(follower))));

    broadcaster.broadcastIfLeader();

    assertEquals(0, follower.getCurrentTerm());
  }
}
