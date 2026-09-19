package com.surajpanda.dmq.raft;

public interface RaftPeerConnection {

  RequestVoteResponse requestVote(RequestVoteRequest request);
}
