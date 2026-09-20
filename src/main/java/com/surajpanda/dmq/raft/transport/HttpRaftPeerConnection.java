package com.surajpanda.dmq.raft.transport;

import com.surajpanda.dmq.raft.RaftPeerConnection;
import com.surajpanda.dmq.raft.RequestVoteRequest;
import com.surajpanda.dmq.raft.RequestVoteResponse;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

/**
 * Real network implementation of the RequestVote channel: POSTs to a peer broker's internal Raft
 * endpoint. A network-level failure (refused/timed-out connection, 5xx, malformed body) is
 * converted into a non-granting vote response rather than thrown, mirroring how the deterministic
 * FaultyRaftPeerConnection test double treats an unavailable peer - callers (ElectionCoordinator)
 * were written against "a peer that doesn't grant a vote", not against transport exceptions, and
 * must never crash the local broker just because one peer is unreachable.
 */
public class HttpRaftPeerConnection implements RaftPeerConnection {

  private final RestClient restClient;
  private final String requestVoteUri;

  public HttpRaftPeerConnection(RestClient restClient, String peerBaseUrl) {
    this.restClient = restClient;
    this.requestVoteUri = peerBaseUrl + "/internal/raft/request-vote";
  }

  @Override
  public RequestVoteResponse requestVote(RequestVoteRequest request) {
    try {
      RequestVoteResponse response =
          restClient
              .post()
              .uri(requestVoteUri)
              .body(request)
              .retrieve()
              .body(RequestVoteResponse.class);

      return response != null ? response : unreachable(request);

    } catch (RestClientException exception) {
      return unreachable(request);
    }
  }

  private static RequestVoteResponse unreachable(RequestVoteRequest request) {
    return new RequestVoteResponse(request.term(), false);
  }
}
