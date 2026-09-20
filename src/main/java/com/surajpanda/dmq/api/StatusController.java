package com.surajpanda.dmq.api;

import com.surajpanda.dmq.broker.BrokerProperties;
import com.surajpanda.dmq.cluster.ClusterProperties;
import com.surajpanda.dmq.raft.RaftNode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Public, read-only cluster/Raft status - added so external callers (operators, or a
 * Testcontainers-based test driving the real HTTP API) can discover the current leader and sanity
 * check cluster health without reaching into this broker's internal Java objects, and without
 * repurposing the internal /internal/raft/* RPC surface or the client-facing /api/v1/messages
 * endpoint as an ad hoc status probe. Never mutates anything.
 */
@RestController
@RequestMapping("/api/v1/status")
public class StatusController {

  private final RaftNode raftNode;
  private final BrokerProperties brokerProperties;
  private final ClusterProperties clusterProperties;

  public StatusController(
      RaftNode raftNode, BrokerProperties brokerProperties, ClusterProperties clusterProperties) {
    this.raftNode = raftNode;
    this.brokerProperties = brokerProperties;
    this.clusterProperties = clusterProperties;
  }

  @GetMapping
  public ResponseEntity<BrokerStatus> status() {
    return ResponseEntity.ok(
        new BrokerStatus(
            brokerProperties.id(),
            raftNode.getState().name(),
            raftNode.getCurrentTerm(),
            raftNode.getCommitIndex(),
            raftNode.getLastApplied(),
            clusterProperties.brokers().size()));
  }
}
