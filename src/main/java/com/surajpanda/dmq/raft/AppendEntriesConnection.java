package com.surajpanda.dmq.raft;

public interface AppendEntriesConnection {

  AppendEntriesResponse sendAppendEntries(AppendEntriesRequest request);
}
