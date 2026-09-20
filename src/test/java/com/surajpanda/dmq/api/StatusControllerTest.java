package com.surajpanda.dmq.api;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.surajpanda.dmq.broker.BrokerProperties;
import com.surajpanda.dmq.cluster.BrokerAddress;
import com.surajpanda.dmq.cluster.ClusterProperties;
import com.surajpanda.dmq.raft.RaftNode;
import com.surajpanda.dmq.raft.RaftState;
import java.util.List;
import org.junit.jupiter.api.Test;

class StatusControllerTest {

  @Test
  void reportsCurrentRaftStateAndConfiguredClusterSize() {
    RaftNode node = new RaftNode("broker-1");
    node.becomeCandidate();
    node.becomeLeader();

    BrokerProperties brokerProperties = new BrokerProperties("broker-1", "broker-1", 8080, "data");
    ClusterProperties clusterProperties =
        new ClusterProperties(
            List.of(
                new BrokerAddress("broker-1", "broker-1", 8080),
                new BrokerAddress("broker-2", "broker-2", 8080),
                new BrokerAddress("broker-3", "broker-3", 8080)));

    StatusController controller = new StatusController(node, brokerProperties, clusterProperties);
    BrokerStatus status = controller.status().getBody();

    assertEquals("broker-1", status.brokerId());
    assertEquals(RaftState.LEADER.name(), status.raftState());
    assertEquals(1, status.currentTerm());
    assertEquals(3, status.configuredClusterSize());
  }
}
