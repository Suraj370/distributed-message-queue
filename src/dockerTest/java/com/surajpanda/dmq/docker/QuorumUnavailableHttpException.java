package com.surajpanda.dmq.docker;

/** Thrown when a publish through the real HTTP API returns 503 (no majority reachable). */
public class QuorumUnavailableHttpException extends RuntimeException {
  public QuorumUnavailableHttpException(String message) {
    super(message);
  }
}
