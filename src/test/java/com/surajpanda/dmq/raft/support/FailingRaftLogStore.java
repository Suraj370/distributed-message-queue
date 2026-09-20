package com.surajpanda.dmq.raft.support;

import com.surajpanda.dmq.raft.LogEntry;
import com.surajpanda.dmq.raft.RaftLogStore;
import com.surajpanda.dmq.raft.RaftPersistenceException;
import java.util.List;

/**
 * Wraps a real RaftLogStore and can be armed to throw on the next replace() call before the
 * delegate is ever invoked - i.e. persistence never even begins. Useful for tests that only care
 * whether the in-memory log stays consistent when persistence fails outright; for tests that need
 * to prove behavior when a failure happens *after* some real bytes have already landed on disk (but
 * before the durable file is actually replaced), use PartiallyFailingRaftLogStore instead - this
 * class cannot represent that scenario.
 */
public class FailingRaftLogStore implements RaftLogStore {

  private final RaftLogStore delegate;
  private volatile boolean failNextReplace = false;

  public FailingRaftLogStore(RaftLogStore delegate) {
    this.delegate = delegate;
  }

  public void failNextReplace() {
    failNextReplace = true;
  }

  @Override
  public void replace(List<LogEntry> fullLog) {
    if (failNextReplace) {
      failNextReplace = false;
      throw new RaftPersistenceException("Simulated persistence failure before any write", null);
    }
    delegate.replace(fullLog);
  }

  @Override
  public List<LogEntry> loadAll() {
    return delegate.loadAll();
  }
}
