package com.surajpanda.dmq.queue;

public class InMemoryLastAppliedStore implements LastAppliedStore {

  private volatile long lastAppliedIndex = 0;

  @Override
  public void save(long lastAppliedIndex) {
    this.lastAppliedIndex = lastAppliedIndex;
  }

  @Override
  public long load() {
    return lastAppliedIndex;
  }
}
