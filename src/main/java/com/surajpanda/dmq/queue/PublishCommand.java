package com.surajpanda.dmq.queue;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * The replicated command representing a client publish request. Encoded into
 * com.surajpanda.dmq.raft.LogEntry's plain-String command field rather than requiring any change to
 * LogEntry itself - key/payload are Base64-encoded before joining with "|" so neither can collide
 * with the delimiter or with each other.
 */
public record PublishCommand(String topic, int partition, String key, String payload) {

  public String encode() {
    return topic + "|" + partition + "|" + encodeBase64(key) + "|" + encodeBase64(payload);
  }

  public static PublishCommand decode(String encoded) {

    String[] parts = encoded.split("\\|", -1);

    if (parts.length != 4) {
      throw new IllegalArgumentException("Invalid PublishCommand encoding: " + encoded);
    }

    return new PublishCommand(
        parts[0], Integer.parseInt(parts[1]), decodeBase64(parts[2]), decodeBase64(parts[3]));
  }

  private static String encodeBase64(String value) {
    return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
  }

  private static String decodeBase64(String value) {
    return new String(Base64.getDecoder().decode(value), StandardCharsets.UTF_8);
  }
}
