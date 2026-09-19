package com.surajpanda.dmq.api;

import com.surajpanda.dmq.queue.NotLeaderException;
import com.surajpanda.dmq.queue.QuorumUnavailableException;
import java.io.IOException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Maps internal exceptions to clean HTTP responses - never a raw stack trace to the client. Raft
 * internals are represented only as HTTP semantics (409/503), never exposed as response fields.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

  @ExceptionHandler(NotLeaderException.class)
  public ResponseEntity<ApiError> handleNotLeader(NotLeaderException exception) {
    return ResponseEntity.status(HttpStatus.CONFLICT)
        .body(new ApiError("NOT_LEADER", "This broker is not currently the Raft leader."));
  }

  @ExceptionHandler(QuorumUnavailableException.class)
  public ResponseEntity<ApiError> handleQuorumUnavailable(QuorumUnavailableException exception) {
    return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
        .body(
            new ApiError(
                "QUORUM_UNAVAILABLE",
                "The request could not be committed by a majority of the cluster."));
  }

  @ExceptionHandler(IllegalArgumentException.class)
  public ResponseEntity<ApiError> handleInvalidRequest(IllegalArgumentException exception) {
    return ResponseEntity.status(HttpStatus.BAD_REQUEST)
        .body(new ApiError("INVALID_REQUEST", exception.getMessage()));
  }

  @ExceptionHandler(IOException.class)
  public ResponseEntity<ApiError> handleIoFailure(IOException exception) {
    return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
        .body(new ApiError("STORAGE_ERROR", "A storage operation failed."));
  }
}
