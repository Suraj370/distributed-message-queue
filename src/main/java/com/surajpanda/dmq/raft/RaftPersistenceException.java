package com.surajpanda.dmq.raft;

/**
 * Raised whenever Raft durable state (term/vote or log) cannot be written or read. Never silently
 * swallowed - a broker that cannot trust its own durable Raft state must not keep participating in
 * elections or replication as if nothing happened.
 */
public class RaftPersistenceException extends RuntimeException {

  public RaftPersistenceException(String message, Throwable cause) {
    super(message, cause);
  }
}
