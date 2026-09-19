package com.surajpanda.dmq.consumer;

/**
 * Raised whenever a consumer group's durable offset state cannot be written or read. Never silently
 * swallowed - losing track of a committed offset could cause silent redelivery or silent skip.
 */
public class OffsetPersistenceException extends RuntimeException {

  public OffsetPersistenceException(String message, Throwable cause) {
    super(message, cause);
  }
}
