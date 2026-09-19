package com.surajpanda.dmq.raft;

public interface HeartbeatConnection {

  HeartbeatResponse sendHeartbeat(Heartbeat heartbeat);
}
