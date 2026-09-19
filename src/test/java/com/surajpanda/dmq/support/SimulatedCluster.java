package com.surajpanda.dmq.support;

import com.surajpanda.dmq.consumer.ConsumerGroup;
import com.surajpanda.dmq.consumer.FileOffsetStore;
import com.surajpanda.dmq.queue.FileLastAppliedStore;
import com.surajpanda.dmq.queue.RaftQueueStateMachine;
import com.surajpanda.dmq.queue.RaftReplicatedQueue;
import com.surajpanda.dmq.raft.AppendEntriesConnection;
import com.surajpanda.dmq.raft.AppendEntriesPeer;
import com.surajpanda.dmq.raft.DurableRaftLog;
import com.surajpanda.dmq.raft.ElectionCoordinator;
import com.surajpanda.dmq.raft.FileRaftLogStore;
import com.surajpanda.dmq.raft.FileRaftMetadataStore;
import com.surajpanda.dmq.raft.RaftLogApplier;
import com.surajpanda.dmq.raft.RaftLogReplicator;
import com.surajpanda.dmq.raft.RaftNode;
import com.surajpanda.dmq.raft.RaftPeer;
import com.surajpanda.dmq.raft.RaftPeerConnection;
import com.surajpanda.dmq.raft.support.FaultyAppendEntriesConnection;
import com.surajpanda.dmq.raft.support.FaultyRaftPeerConnection;
import com.surajpanda.dmq.topic.TopicManager;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A deterministic, in-process simulation of an N-node Raft-backed queue cluster, for the
 * distributed failure tests. Every peer connection is wrapped in a Faulty*Connection so a node can
 * be marked down/up on demand (no real networking, no randomness, no sleeping).
 *
 * <p>Every node's Raft metadata, Raft log, and queue applied-index are backed by real files under
 * its own directory, so restartNode() is a genuine restart: the old in-memory objects are discarded
 * and new ones are built by recovering from disk, exactly as a real process restart would.
 */
public final class SimulatedCluster {

  public record Node(
      String id,
      RaftNode raftNode,
      RaftLogReplicator replicator,
      ElectionCoordinator election,
      RaftQueueStateMachine stateMachine,
      RaftLogApplier applier,
      RaftReplicatedQueue queue,
      TopicManager topicManager,
      ConsumerGroup consumerGroup,
      Map<String, FaultyRaftPeerConnection> outboundVoteConnections,
      Map<String, FaultyAppendEntriesConnection> outboundAppendConnections) {}

  private final List<Node> nodes = new ArrayList<>();
  private final Path baseDir;
  private final String topicName;
  private final int partitionCount;
  private final int clusterSize;

  public SimulatedCluster(int size, Path baseDir, String topicName, int partitionCount)
      throws IOException {
    this.baseDir = baseDir;
    this.topicName = topicName;
    this.partitionCount = partitionCount;
    this.clusterSize = size;

    for (int i = 0; i < size; i++) {
      nodes.add(buildNode(i));
    }
  }

  private Node buildNode(int index) throws IOException {

    String id = "node-" + index;
    Path nodeDir = baseDir.resolve(id);

    RaftNode raftNode =
        new RaftNode(
            id,
            new FileRaftMetadataStore(nodeDir.resolve("raft-meta")),
            new DurableRaftLog(new FileRaftLogStore(nodeDir.resolve("raft-log"))));

    TopicManager topicManager = new TopicManager(nodeDir.resolve("topics"));
    if (topicManager.getTopic(topicName) == null) {
      topicManager.createTopic(topicName, partitionCount);
    }

    List<RaftPeer> votePeers = new ArrayList<>();
    List<AppendEntriesPeer> appendPeers = new ArrayList<>();
    Map<String, FaultyRaftPeerConnection> outboundVoteConnections = new LinkedHashMap<>();
    Map<String, FaultyAppendEntriesConnection> outboundAppendConnections = new LinkedHashMap<>();

    for (int j = 0; j < clusterSize; j++) {
      if (j == index) {
        continue;
      }
      int peerIndex = j;
      String peerId = "node-" + peerIndex;

      RaftPeerConnection dynamicVote =
          request -> nodes.get(peerIndex).raftNode().handleRequestVote(request);
      AppendEntriesConnection dynamicAppend =
          request -> nodes.get(peerIndex).raftNode().handleAppendEntries(request);

      FaultyRaftPeerConnection voteConnection = new FaultyRaftPeerConnection(dynamicVote);
      FaultyAppendEntriesConnection appendConnection =
          new FaultyAppendEntriesConnection(dynamicAppend);

      outboundVoteConnections.put(peerId, voteConnection);
      outboundAppendConnections.put(peerId, appendConnection);

      votePeers.add(new RaftPeer(peerId, voteConnection));
      appendPeers.add(new AppendEntriesPeer(peerId, appendConnection));
    }

    ElectionCoordinator election = new ElectionCoordinator(raftNode, votePeers, clusterSize);
    RaftLogReplicator replicator = new RaftLogReplicator(raftNode, appendPeers, clusterSize);

    RaftQueueStateMachine stateMachine =
        new RaftQueueStateMachine(
            topicManager, new FileLastAppliedStore(nodeDir.resolve("last-applied")));
    RaftLogApplier applier = new RaftLogApplier(raftNode, stateMachine);
    RaftReplicatedQueue queue =
        new RaftReplicatedQueue(raftNode, replicator, applier, stateMachine);

    ConsumerGroup consumerGroup =
        new ConsumerGroup("test-group", new FileOffsetStore(nodeDir.resolve("offsets")));

    return new Node(
        id,
        raftNode,
        replicator,
        election,
        stateMachine,
        applier,
        queue,
        topicManager,
        consumerGroup,
        outboundVoteConnections,
        outboundAppendConnections);
  }

  public Node node(int index) {
    return nodes.get(index);
  }

  public int size() {
    return clusterSize;
  }

  /**
   * Elects the given node as leader via a real election against currently reachable peers, and
   * initializes its per-peer replication progress. Fails the test-setup step if it doesn't win.
   */
  public void electLeader(int leaderIndex) {
    Node leader = nodes.get(leaderIndex);
    boolean elected = leader.election().startElection();
    if (!elected) {
      throw new IllegalStateException(id(leaderIndex) + " failed to win the election");
    }
    leader.replicator().initializeForNewLeader();
  }

  /**
   * Leader replicates and, if a majority accepted, commits; a second round lets followers learn the
   * new commitIndex, then every reachable node applies whatever it has committed.
   */
  public void replicateAndCommit(int leaderIndex) {
    Node leader = nodes.get(leaderIndex);
    leader.replicator().replicate(leader.raftNode().getCommitIndex());
    leader.replicator().replicate(leader.raftNode().getCommitIndex());
    for (Node n : nodes) {
      n.applier().applyCommitted();
    }
  }

  /**
   * Simulates node[index] becoming completely unavailable: every other node's outbound connections
   * to it stop delivering, and its own outbound connections to others also stop (a true crash, not
   * a one-way partition), without discarding any of its durable state.
   */
  public void takeDown(int index) {
    setReachability(index, false);
  }

  public void bringUp(int index) {
    setReachability(index, true);
  }

  private void setReachability(int index, boolean available) {
    for (int i = 0; i < clusterSize; i++) {
      if (i != index) {
        setLinkAvailable(index, i, available);
      }
    }
  }

  /**
   * Fine-grained control over a single pair's link, independent of every other node's reachability
   * - use this when a test needs to isolate one node while two others stay connected to each other.
   */
  public void setLinkAvailable(int aIndex, int bIndex, boolean available) {

    Node a = nodes.get(aIndex);
    Node b = nodes.get(bIndex);
    String aId = id(aIndex);
    String bId = id(bIndex);

    var aToB = a.outboundVoteConnections().get(bId);
    if (aToB != null) {
      aToB.setAvailable(available);
    }
    var aToBAppend = a.outboundAppendConnections().get(bId);
    if (aToBAppend != null) {
      aToBAppend.setAvailable(available);
    }
    var bToA = b.outboundVoteConnections().get(aId);
    if (bToA != null) {
      bToA.setAvailable(available);
    }
    var bToAAppend = b.outboundAppendConnections().get(aId);
    if (bToAAppend != null) {
      bToAAppend.setAvailable(available);
    }
  }

  /**
   * Discards node[index]'s in-memory Raft/queue objects entirely and rebuilds them from disk - a
   * genuine restart. Other nodes' connections automatically start talking to the new instance (they
   * look it up from this cluster's node list at call time rather than holding a fixed RaftNode
   * reference), and the restarted node's own outbound connections are rebuilt fresh (reachable by
   * default, matching a real process coming back up able to dial out).
   */
  public void restartNode(int index) {
    try {
      nodes.set(index, buildNode(index));
    } catch (IOException exception) {
      throw new UncheckedIOException(exception);
    }
    setReachability(index, true); // a restarted node has rejoined - reachable again by default
  }

  public String id(int index) {
    return "node-" + index;
  }
}
