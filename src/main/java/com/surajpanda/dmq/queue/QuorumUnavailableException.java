package com.surajpanda.dmq.queue;

/**
 * Thrown when a proposed command could not be replicated to a majority of the configured Raft
 * cluster, so it never became committed. The command was never applied to queue state.
 */
public class QuorumUnavailableException extends RuntimeException {

  public QuorumUnavailableException(String message) {
    super(message);
  }
}
