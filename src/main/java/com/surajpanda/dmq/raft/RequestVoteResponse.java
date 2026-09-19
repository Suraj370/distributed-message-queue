package com.surajpanda.dmq.raft;

public record RequestVoteResponse(long term, boolean voteGranted) {}
