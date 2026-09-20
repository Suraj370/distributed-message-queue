package com.surajpanda.dmq.docker;

/** Mirrors com.surajpanda.dmq.api.BrokerStatus's JSON shape - deliberately not shared with main. */
public record StatusResponse(
    String brokerId,
    String raftState,
    long currentTerm,
    long commitIndex,
    long lastApplied,
    int configuredClusterSize) {}
