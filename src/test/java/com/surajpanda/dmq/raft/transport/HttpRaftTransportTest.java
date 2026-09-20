package com.surajpanda.dmq.raft.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import com.surajpanda.dmq.raft.AppendEntriesRequest;
import com.surajpanda.dmq.raft.AppendEntriesResponse;
import com.surajpanda.dmq.raft.Heartbeat;
import com.surajpanda.dmq.raft.HeartbeatResponse;
import com.surajpanda.dmq.raft.LogEntry;
import com.surajpanda.dmq.raft.RequestVoteRequest;
import com.surajpanda.dmq.raft.RequestVoteResponse;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

/**
 * Verifies the actual network behavior of the HTTP Raft transport - a real local HTTP server for
 * the success path, a closed port for "connection refused", and a deliberately malformed response
 * body for "unparseable remote response" - and that every one of those failure modes is converted
 * into a safe non-granting/non-success response rather than an exception, per RaftPeerConnection /
 * AppendEntriesConnection / HeartbeatConnection's contract of never crashing the caller.
 */
class HttpRaftTransportTest {

  private HttpServer server;

  @AfterEach
  void stopServer() {
    if (server != null) {
      server.stop(0);
    }
  }

  private String startServer(String path, int status, String body) throws IOException {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        path,
        exchange -> {
          byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          exchange.sendResponseHeaders(status, bytes.length);
          exchange.getResponseBody().write(bytes);
          exchange.close();
        });
    server.start();
    return "http://localhost:" + server.getAddress().getPort();
  }

  private static RestClient restClient() {
    SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
    factory.setConnectTimeout(500);
    factory.setReadTimeout(500);
    return RestClient.builder().requestFactory(factory).build();
  }

  // --- RequestVote -----------------------------------------------------------------------

  @Test
  void requestVoteReturnsTheRealRemoteResponseOnSuccess() throws Exception {
    String baseUrl =
        startServer("/internal/raft/request-vote", 200, "{\"term\":7,\"voteGranted\":true}");

    HttpRaftPeerConnection connection = new HttpRaftPeerConnection(restClient(), baseUrl);
    RequestVoteResponse response =
        connection.requestVote(new RequestVoteRequest(7, "broker-1", 0, 0));

    assertEquals(7, response.term());
    assertTrue(response.voteGranted());
  }

  @Test
  void requestVoteToAnUnreachablePeerReturnsNonGrantingResponseInstead() {
    // Nothing is listening on this port - a genuine connection-refused case.
    HttpRaftPeerConnection connection =
        new HttpRaftPeerConnection(restClient(), "http://localhost:1");

    RequestVoteResponse response =
        connection.requestVote(new RequestVoteRequest(3, "broker-1", 0, 0));

    assertEquals(3, response.term());
    assertFalse(response.voteGranted());
  }

  @Test
  void requestVoteWithAMalformedRemoteResponseReturnsNonGrantingResponseInstead() throws Exception {
    String baseUrl = startServer("/internal/raft/request-vote", 200, "not-json-at-all");

    HttpRaftPeerConnection connection = new HttpRaftPeerConnection(restClient(), baseUrl);
    RequestVoteResponse response =
        connection.requestVote(new RequestVoteRequest(4, "broker-1", 0, 0));

    assertEquals(4, response.term());
    assertFalse(response.voteGranted());
  }

  @Test
  void requestVoteWithARemoteServerErrorReturnsNonGrantingResponseInstead() throws Exception {
    String baseUrl = startServer("/internal/raft/request-vote", 500, "{}");

    HttpRaftPeerConnection connection = new HttpRaftPeerConnection(restClient(), baseUrl);
    RequestVoteResponse response =
        connection.requestVote(new RequestVoteRequest(2, "broker-1", 0, 0));

    assertEquals(2, response.term());
    assertFalse(response.voteGranted());
  }

  // --- AppendEntries -----------------------------------------------------------------------

  @Test
  void appendEntriesReturnsTheRealRemoteResponseOnSuccess() throws Exception {
    String baseUrl =
        startServer(
            "/internal/raft/append-entries",
            200,
            "{\"term\":5,\"termAccepted\":true,\"success\":true,\"matchIndex\":3}");

    HttpAppendEntriesConnection connection = new HttpAppendEntriesConnection(restClient(), baseUrl);
    AppendEntriesResponse response =
        connection.sendAppendEntries(
            new AppendEntriesRequest(5, "broker-1", 0, 0, List.of(new LogEntry(5, 1, "cmd")), 0));

    assertEquals(5, response.term());
    assertTrue(response.termAccepted());
    assertTrue(response.success());
    assertEquals(3, response.matchIndex());
  }

  @Test
  void appendEntriesToAnUnreachablePeerReturnsAFailureResponseInstead() {
    HttpAppendEntriesConnection connection =
        new HttpAppendEntriesConnection(restClient(), "http://localhost:1");

    AppendEntriesResponse response =
        connection.sendAppendEntries(new AppendEntriesRequest(6, "broker-1", 0, 0, List.of(), 0));

    assertEquals(6, response.term());
    assertFalse(response.termAccepted());
    assertFalse(response.success());
  }

  @Test
  void appendEntriesWithAMalformedRemoteResponseReturnsAFailureResponseInstead() throws Exception {
    String baseUrl = startServer("/internal/raft/append-entries", 200, "<<not json>>");

    HttpAppendEntriesConnection connection = new HttpAppendEntriesConnection(restClient(), baseUrl);
    AppendEntriesResponse response =
        connection.sendAppendEntries(new AppendEntriesRequest(1, "broker-1", 0, 0, List.of(), 0));

    assertEquals(1, response.term());
    assertFalse(response.success());
  }

  // --- Heartbeat -----------------------------------------------------------------------

  @Test
  void heartbeatReturnsTheRealRemoteResponseOnSuccess() throws Exception {
    String baseUrl = startServer("/internal/raft/heartbeat", 200, "{\"term\":9,\"accepted\":true}");

    HttpHeartbeatConnection connection = new HttpHeartbeatConnection(restClient(), baseUrl);
    HeartbeatResponse response = connection.sendHeartbeat(new Heartbeat(9, "broker-1"));

    assertEquals(9, response.term());
    assertTrue(response.accepted());
  }

  @Test
  void heartbeatToAnUnreachablePeerReturnsANotAcceptedResponseInstead() {
    HttpHeartbeatConnection connection =
        new HttpHeartbeatConnection(restClient(), "http://localhost:1");

    HeartbeatResponse response = connection.sendHeartbeat(new Heartbeat(8, "broker-1"));

    assertEquals(8, response.term());
    assertFalse(response.accepted());
  }
}
