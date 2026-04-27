package com.example.orders.service;

import com.example.orders.config.RedisStreamsProperties;
import com.example.orders.dto.CreateOrderRequest;
import com.example.orders.dto.CreateOrderResponse;
import com.example.orders.model.OrderCreatedEvent;
import com.example.orders.redis.OrderEventJsonCodec;
import com.example.orders.support.StreamNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Service
public class OrderService {
    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final RedisTemplate<String, String> redisTemplate;

    private final RedisStreamsProperties properties;

    private final OrderEventJsonCodec codec;

    public OrderService(
            RedisTemplate<String, String> redisTemplate,
            RedisStreamsProperties properties,
            OrderEventJsonCodec codec
    ) {
        this.redisTemplate = redisTemplate;

        this.properties = properties;

        this.codec = codec;
    }

    public CreateOrderResponse createOrder(CreateOrderRequest request) {
        UUID orderId = UUID.randomUUID();

        OrderCreatedEvent event = new OrderCreatedEvent(
                orderId,
                request.userId(),
                request.productCode(),
                request.quantity(),
                request.totalPrice(),
                Instant.now()
        );

        String payload = codec.encode(event);

        Map<String, String> body = Map.of(
                StreamNames.PAYLOAD_FIELD,
                payload
        );

        RecordId recordId = redisTemplate
                .opsForStream()
                .add(
                        StreamRecords
                                .mapBacked(body)
                                .withStreamKey(properties.ordersStream())
                );


        log.info(
                "Order event published: orderId={}, redisRecordId={}, stream={}",
                orderId,
                recordId,
                properties.ordersStream()
        );

        return new CreateOrderResponse(orderId, "CREATED");
    }
}
