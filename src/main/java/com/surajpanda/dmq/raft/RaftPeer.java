package com.surajpanda.dmq.raft;

public record RaftPeer(String nodeId, RaftPeerConnection connection) {}
