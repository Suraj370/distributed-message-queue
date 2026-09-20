package com.surajpanda.dmq.docker;

/** Mirrors com.surajpanda.dmq.message.Message's JSON shape - deliberately not shared with main. */
public record MessageResponse(
    String id, String key, String payload, String timestamp, int partition, long offset) {}
