package com.surajpanda.dmq.replication;

public record ReplicaTarget(String brokerId, ReplicaConnection connection) {}
