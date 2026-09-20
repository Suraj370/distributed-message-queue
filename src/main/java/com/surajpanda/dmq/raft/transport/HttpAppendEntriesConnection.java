package com.surajpanda.dmq.raft.transport;

import com.surajpanda.dmq.raft.AppendEntriesConnection;
import com.surajpanda.dmq.raft.AppendEntriesRequest;
import com.surajpanda.dmq.raft.AppendEntriesResponse;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

/**
 * Real network implementation of the AppendEntries channel (also carries heartbeats-with-entries
 * once a leader is established). A network-level failure never crashes the leader's replication
 * loop and is never mistaken for a legitimate higher-term response: term is echoed back from the
 * request (never advances the caller's term), termAccepted/success are both false, matchIndex is 0
 * - the same shape RaftLogReplicator already treats as "this peer needs a retry/backtrack", just
 * without pretending the peer is caught up.
 */
public class HttpAppendEntriesConnection implements AppendEntriesConnection {

  private final RestClient restClient;
  private final String appendEntriesUri;

  public HttpAppendEntriesConnection(RestClient restClient, String peerBaseUrl) {
    this.restClient = restClient;
    this.appendEntriesUri = peerBaseUrl + "/internal/raft/append-entries";
  }

  @Override
  public AppendEntriesResponse sendAppendEntries(AppendEntriesRequest request) {
    try {
      AppendEntriesResponse response =
          restClient
              .post()
              .uri(appendEntriesUri)
              .body(request)
              .retrieve()
              .body(AppendEntriesResponse.class);

      return response != null ? response : unreachable(request);

    } catch (RestClientException exception) {
      return unreachable(request);
    }
  }

  private static AppendEntriesResponse unreachable(AppendEntriesRequest request) {
    return new AppendEntriesResponse(request.term(), false, false, 0);
  }
}
