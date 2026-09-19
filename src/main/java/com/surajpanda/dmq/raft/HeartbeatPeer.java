package com.surajpanda.dmq.raft;

public record HeartbeatPeer(String nodeId, HeartbeatConnection connection) {}
