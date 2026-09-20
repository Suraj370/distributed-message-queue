package com.surajpanda.dmq.api;

/**
 * Read-only cluster/Raft observability, safe to expose publicly: no mutation, no internals beyond
 * what a client legitimately needs to discover the leader and sanity-check cluster health over HTTP
 * - the same information a real operator or a Testcontainers-style external test would need,
 * without reaching into this process's Java objects directly.
 */
public record BrokerStatus(
    String brokerId,
    String raftState,
    long currentTerm,
    long commitIndex,
    long lastApplied,
    int configuredClusterSize) {}
