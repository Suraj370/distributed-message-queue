package com.surajpanda.dmq.replication;

public record ReplicationOutcome(boolean success, Throwable error) {

  public static ReplicationOutcome ok() {
    return new ReplicationOutcome(true, null);
  }

  public static ReplicationOutcome failed(Throwable error) {
    return new ReplicationOutcome(false, error);
  }
}
