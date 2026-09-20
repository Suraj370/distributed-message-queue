package com.surajpanda.dmq.raft.transport;

import com.surajpanda.dmq.raft.Heartbeat;
import com.surajpanda.dmq.raft.HeartbeatConnection;
import com.surajpanda.dmq.raft.HeartbeatResponse;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

/**
 * Real network implementation of the Heartbeat channel. A network-level failure is converted into a
 * not-accepted response at the request's own term (never a manufactured higher term), so
 * HeartbeatBroadcaster's step-down check (response.term() > currentTerm) is never falsely triggered
 * by an unreachable peer.
 */
public class HttpHeartbeatConnection implements HeartbeatConnection {

  private final RestClient restClient;
  private final String heartbeatUri;

  public HttpHeartbeatConnection(RestClient restClient, String peerBaseUrl) {
    this.restClient = restClient;
    this.heartbeatUri = peerBaseUrl + "/internal/raft/heartbeat";
  }

  @Override
  public HeartbeatResponse sendHeartbeat(Heartbeat heartbeat) {
    try {
      HeartbeatResponse response =
          restClient
              .post()
              .uri(heartbeatUri)
              .body(heartbeat)
              .retrieve()
              .body(HeartbeatResponse.class);

      return response != null ? response : unreachable(heartbeat);

    } catch (RestClientException exception) {
      return unreachable(heartbeat);
    }
  }

  private static HeartbeatResponse unreachable(Heartbeat heartbeat) {
    return new HeartbeatResponse(heartbeat.term(), false);
  }
}
