package com.surajpanda.dmq.queue;

/**
 * Raised whenever queue-side durable state (the state machine's applied-index tracking) cannot be
 * written or read. Never silently swallowed.
 */
public class QueuePersistenceException extends RuntimeException {

  public QueuePersistenceException(String message, Throwable cause) {
    super(message, cause);
  }
}
