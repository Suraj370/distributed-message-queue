package com.surajpanda.dmq.queue;

/**
 * Thrown when a publish is requested against a broker that is not the current Raft leader. A
 * follower must never accept normal client publishing directly.
 */
public class NotLeaderException extends RuntimeException {

  public NotLeaderException(String message) {
    super(message);
  }
}
