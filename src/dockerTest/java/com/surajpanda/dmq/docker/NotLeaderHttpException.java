package com.surajpanda.dmq.docker;

/** Thrown when a publish through the real HTTP API returns 409 (broker is not the leader). */
public class NotLeaderHttpException extends RuntimeException {
  public NotLeaderHttpException(String message) {
    super(message);
  }
}
