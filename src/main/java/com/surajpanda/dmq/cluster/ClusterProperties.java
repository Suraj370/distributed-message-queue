package com.surajpanda.dmq.cluster;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "cluster")
public record ClusterProperties(@NotEmpty @Valid @UniqueBrokerIds List<BrokerAddress> brokers) {}
