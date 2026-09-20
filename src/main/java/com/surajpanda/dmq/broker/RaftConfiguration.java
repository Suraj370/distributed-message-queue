package com.surajpanda.dmq.broker;

import com.surajpanda.dmq.cluster.BrokerAddress;
import com.surajpanda.dmq.cluster.ClusterProperties;
import com.surajpanda.dmq.queue.FileLastAppliedStore;
import com.surajpanda.dmq.queue.RaftQueueStateMachine;
import com.surajpanda.dmq.queue.RaftReplicatedQueue;
import com.surajpanda.dmq.raft.AppendEntriesPeer;
import com.surajpanda.dmq.raft.DurableRaftLog;
import com.surajpanda.dmq.raft.ElectionCoordinator;
import com.surajpanda.dmq.raft.ElectionTimeout;
import com.surajpanda.dmq.raft.FileRaftLogStore;
import com.surajpanda.dmq.raft.FileRaftMetadataStore;
import com.surajpanda.dmq.raft.HeartbeatBroadcaster;
import com.surajpanda.dmq.raft.HeartbeatPeer;
import com.surajpanda.dmq.raft.RaftElectionDriver;
import com.surajpanda.dmq.raft.RaftLogApplier;
import com.surajpanda.dmq.raft.RaftLogReplicator;
import com.surajpanda.dmq.raft.RaftNode;
import com.surajpanda.dmq.raft.RaftNodeScheduler;
import com.surajpanda.dmq.raft.RaftPeer;
import com.surajpanda.dmq.raft.RaftState;
import com.surajpanda.dmq.raft.transport.HttpAppendEntriesConnection;
import com.surajpanda.dmq.raft.transport.HttpHeartbeatConnection;
import com.surajpanda.dmq.raft.transport.HttpRaftPeerConnection;
import com.surajpanda.dmq.raft.transport.RaftProperties;
import com.surajpanda.dmq.topic.TopicManager;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import java.nio.file.Path;
import java.time.Clock;
import java.util.List;
import java.util.Random;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

/**
 * Wires this broker's real, multi-broker Raft cluster: an HTTP transport to every other configured
 * broker (see the raft.transport package), election/replication sized off the full configured
 * cluster membership (never off how many peers currently happen to be reachable), and the scheduler
 * that actually drives elections and heartbeats - and, via its onTick hook, periodic catch-up
 * replication while leader - in a running process.
 *
 * <p>Configured membership is authoritative: clusterSize always comes from
 * clusterProperties.brokers().size(), never from peers.size() + 1 - a broker that is down or
 * unreachable is still a configured voting member, so the majority requirement must not silently
 * shrink just because it can't currently be dialed (see ElectionCoordinator/RaftLogReplicator's own
 * javadoc for why that distinction matters).
 */
@Configuration
public class RaftConfiguration {

  @Bean
  public Clock raftClock() {
    return Clock.systemUTC();
  }

  @Bean
  public RaftNode raftNode(BrokerProperties brokerProperties) {

    Path raftDirectory = Path.of(brokerProperties.dataDirectory()).resolve("raft");

    // A recovered node always starts FOLLOWER (see RaftNode's own javadoc) and joins real
    // elections through the scheduler below - no self-election shortcut anymore.
    return new RaftNode(
        brokerProperties.id(),
        new FileRaftMetadataStore(raftDirectory.resolve("meta")),
        new DurableRaftLog(new FileRaftLogStore(raftDirectory.resolve("log"))));
  }

  @Bean
  public List<BrokerAddress> remoteBrokers(
      BrokerProperties brokerProperties, ClusterProperties clusterProperties) {
    return clusterProperties.brokers().stream()
        .filter(address -> !address.id().equals(brokerProperties.id()))
        .toList();
  }

  @Bean
  public RestClient raftRestClient(RaftProperties raftProperties) {

    SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
    requestFactory.setConnectTimeout((int) raftProperties.transport().connectTimeoutMillis());
    requestFactory.setReadTimeout((int) raftProperties.transport().readTimeoutMillis());

    return RestClient.builder().requestFactory(requestFactory).build();
  }

  @Bean
  public List<RaftPeer> raftVotePeers(
      List<BrokerAddress> remoteBrokers, RestClient raftRestClient) {
    return remoteBrokers.stream()
        .map(
            address ->
                new RaftPeer(
                    address.id(), new HttpRaftPeerConnection(raftRestClient, baseUrl(address))))
        .toList();
  }

  @Bean
  public List<AppendEntriesPeer> raftAppendPeers(
      List<BrokerAddress> remoteBrokers, RestClient raftRestClient) {
    return remoteBrokers.stream()
        .map(
            address ->
                new AppendEntriesPeer(
                    address.id(),
                    new HttpAppendEntriesConnection(raftRestClient, baseUrl(address))))
        .toList();
  }

  @Bean
  public List<HeartbeatPeer> raftHeartbeatPeers(
      List<BrokerAddress> remoteBrokers, RestClient raftRestClient) {
    return remoteBrokers.stream()
        .map(
            address ->
                new HeartbeatPeer(
                    address.id(), new HttpHeartbeatConnection(raftRestClient, baseUrl(address))))
        .toList();
  }

  @Bean
  public ElectionCoordinator electionCoordinator(
      RaftNode raftNode, List<RaftPeer> raftVotePeers, ClusterProperties clusterProperties) {
    return new ElectionCoordinator(raftNode, raftVotePeers, clusterProperties.brokers().size());
  }

  @Bean
  public RaftLogReplicator raftLogReplicator(
      RaftNode raftNode,
      List<AppendEntriesPeer> raftAppendPeers,
      ClusterProperties clusterProperties,
      MeterRegistry meterRegistry) {
    return new RaftLogReplicator(
        raftNode, raftAppendPeers, clusterProperties.brokers().size(), meterRegistry);
  }

  @Bean
  public HeartbeatBroadcaster heartbeatBroadcaster(
      RaftNode raftNode, List<HeartbeatPeer> raftHeartbeatPeers) {
    return new HeartbeatBroadcaster(raftNode, raftHeartbeatPeers);
  }

  @Bean
  public ElectionTimeout electionTimeout(RaftProperties raftProperties) {
    return new ElectionTimeout(
        raftProperties.election().minTimeoutMillis(),
        raftProperties.election().maxTimeoutMillis(),
        new Random());
  }

  @Bean
  public RaftElectionDriver raftElectionDriver(
      RaftNode raftNode,
      ElectionCoordinator electionCoordinator,
      ElectionTimeout electionTimeout,
      RaftLogReplicator raftLogReplicator,
      Clock raftClock) {
    return new RaftElectionDriver(
        raftNode,
        electionCoordinator,
        electionTimeout,
        raftClock.millis(),
        raftLogReplicator::initializeForNewLeader);
  }

  @Bean(destroyMethod = "close")
  public RaftNodeScheduler raftNodeScheduler(
      RaftNode raftNode,
      RaftElectionDriver raftElectionDriver,
      HeartbeatBroadcaster heartbeatBroadcaster,
      RaftLogReplicator raftLogReplicator,
      RaftProperties raftProperties,
      Clock raftClock) {

    // Drives catch-up replication on every tick while this node is leader - not just when a
    // client publish happens to trigger one - so a follower that fell behind (e.g. it just
    // restarted) converges on its own. Harmless when there is nothing new to send: replicate()
    // just re-confirms matchIndex/leaderCommit for already-caught-up peers.
    Runnable replicateIfLeader =
        () -> {
          if (raftNode.getState() == RaftState.LEADER) {
            raftLogReplicator.replicate(raftNode.getCommitIndex());
          }
        };

    RaftNodeScheduler scheduler =
        new RaftNodeScheduler(
            raftElectionDriver,
            heartbeatBroadcaster,
            raftProperties.election().minTimeoutMillis(),
            raftProperties.heartbeatIntervalMillis(),
            raftClock,
            replicateIfLeader);

    scheduler.start();

    return scheduler;
  }

  /**
   * Registers this broker's Raft state as Micrometer gauges - term, commit index, last-applied
   * index, and one gauge per {@link RaftState} value (1 for whichever state this node is currently
   * in, 0 for the others - the standard Prometheus pattern for an enum-like value, since a single
   * gauge can't hold a state name). Purely observational: it only reads RaftNode's existing getters
   * via a Spring Boot {@link MeterBinder} bean, so RaftNode itself needed no changes at all.
   */
  @Bean
  public MeterBinder raftStateMetrics(RaftNode raftNode, BrokerProperties brokerProperties) {
    return (MeterRegistry registry) -> {
      Gauge.builder("dmq_raft_term", raftNode, RaftNode::getCurrentTerm)
          .description("Current Raft term of this broker")
          .register(registry);
      Gauge.builder("dmq_raft_commit_index", raftNode, RaftNode::getCommitIndex)
          .description("Highest Raft log index known to be committed on this broker")
          .register(registry);
      Gauge.builder("dmq_raft_last_applied_index", raftNode, RaftNode::getLastApplied)
          .description("Highest Raft log index applied to queue state on this broker")
          .register(registry);
      for (RaftState state : RaftState.values()) {
        Gauge.builder("dmq_raft_state", raftNode, node -> node.getState() == state ? 1 : 0)
            .description("1 if this broker is currently in the tagged raft_state, else 0")
            .tag("raft_state", state.name())
            .register(registry);
      }
    };
  }

  @Bean
  public RaftQueueStateMachine raftQueueStateMachine(
      TopicManager topicManager, BrokerProperties brokerProperties, MeterRegistry meterRegistry) {
    Path lastAppliedFile =
        Path.of(brokerProperties.dataDirectory()).resolve("raft").resolve("last-applied");
    return new RaftQueueStateMachine(
        topicManager, new FileLastAppliedStore(lastAppliedFile), meterRegistry);
  }

  @Bean
  public RaftLogApplier raftLogApplier(RaftNode raftNode, RaftQueueStateMachine stateMachine) {
    return new RaftLogApplier(raftNode, stateMachine);
  }

  @Bean
  public RaftReplicatedQueue raftReplicatedQueue(
      RaftNode raftNode,
      RaftLogReplicator replicator,
      RaftLogApplier applier,
      RaftQueueStateMachine stateMachine) {
    return new RaftReplicatedQueue(raftNode, replicator, applier, stateMachine);
  }

  private static String baseUrl(BrokerAddress address) {
    return "http://" + address.host() + ":" + address.port();
  }
}
