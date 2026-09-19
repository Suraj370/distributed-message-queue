package com.surajpanda.dmq.raft;

/**
 * A single Raft log entry. Deliberately independent of the queue's Message/WAL model: a Raft log
 * index is a coordination-log position, not a Partition offset, even though a later milestone may
 * decide how one maps onto the other. The command payload is an opaque placeholder for whatever
 * gets replicated - it is not itself a queue Message.
 */
public record LogEntry(long term, long index, String command) {}
