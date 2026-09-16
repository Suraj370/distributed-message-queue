package com.surajpanda.dmq.cluster;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public record BrokerAddress(
    @NotBlank String id, @NotBlank String host, @Min(1) @Max(65535) int port) {}
