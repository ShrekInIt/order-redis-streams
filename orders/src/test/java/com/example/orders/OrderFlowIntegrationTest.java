package com.example.orders;

import com.example.orders.config.RedisStreamsProperties;
import com.example.orders.support.StreamNames;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class OrderFlowIntegrationTest {
    @Container
    static GenericContainer<?> redis = new GenericContainer<>("redis:7.4-alpine")
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void redisProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", redis::getHost);

        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));

        registry.add("app.redis-streams.claim-idle-time-ms", () -> "200");

        registry.add("app.redis-streams.pending-check-delay-ms", () -> "200");
    }

    @LocalServerPort
    int port;
    @Autowired
    TestRestTemplate restTemplate;

    @Autowired
    RedisTemplate<String, String> redisTemplate;
    @Autowired
    RedisStreamsProperties properties;

    @Test
    void shouldCreateOrderAndProcessMessageSuccessfully() {
        Map<String, Object> request = Map.of(
                "userId", "user-1",
                "productCode", "BOOK-1",
                "quantity", 2,
                "totalPrice", 19.99
        );

        var response = restTemplate.postForEntity(
                "http://localhost:" + port + "/api/orders",
                request,
                Map.class
        );
        assertThat(response.getStatusCode().value()).isEqualTo(201);

        assertThat(response.getBody()).containsKey("orderId");
    }
    @Test
    void shouldMovePoisonMessageToDlqAfterRetries() {
        Map<String, Object> request = Map.of(
                "userId", "user-2",
                "productCode", "EXPENSIVE-ITEM",
                "quantity", 101,
                "totalPrice", 999.99
        );

        var response = restTemplate.postForEntity(
                "http://localhost:" + port + "/api/orders",
                request,
                Map.class
        );

        assertThat(response.getStatusCode().value()).isEqualTo(201);

        await()
                .atMost(java.time.Duration.ofSeconds(10))
                .untilAsserted(() -> {
                    List<MapRecord<String, Object, Object>> dlqRecords = redisTemplate
                            .opsForStream()
                            .range(properties.dlqStream(), org.springframework.data.domain.Range.unbounded());

                    assertThat(dlqRecords).isNotNull();
                    assertThat(dlqRecords).isNotEmpty();
                    MapRecord<String, Object, Object> first = dlqRecords.get(0);
                    assertThat(first.getValue()).containsKey(StreamNames.PAYLOAD_FIELD);
                    assertThat(first.getValue()).containsKey(StreamNames.ERROR_FIELD);
                });
    }
}
