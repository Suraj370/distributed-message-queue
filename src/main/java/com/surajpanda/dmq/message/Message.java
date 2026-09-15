package com.surajpanda.dmq.message;

import java.time.Instant;
import java.util.UUID;

public record Message(
        UUID id,
        String key,
        String payload,
        Instant timestamp
) {
    public static Message create(String key, String payload) {
        return new Message(
                UUID.randomUUID(),
                key,
                payload,
                Instant.now()
        );
    }
}