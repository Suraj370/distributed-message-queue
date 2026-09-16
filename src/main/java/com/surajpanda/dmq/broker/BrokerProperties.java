package com.surajpanda.dmq.broker;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "broker")
public record BrokerProperties(
    @NotBlank String id,
    @NotBlank String host,
    @Min(1) @Max(65535) int port,
    @NotBlank String dataDirectory) {}
