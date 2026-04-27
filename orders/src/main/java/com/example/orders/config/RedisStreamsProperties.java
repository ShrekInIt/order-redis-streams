package com.example.orders.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.redis-streams")
public record RedisStreamsProperties(
        String ordersStream,
        String dlqStream,
        String consumerGroup,
        String consumerName,
        int maxRetryAttempts,
        long claimIdleTimeMs,
        long pendingCheckDelayMs,
        long pendingBatchSize
) {
}
