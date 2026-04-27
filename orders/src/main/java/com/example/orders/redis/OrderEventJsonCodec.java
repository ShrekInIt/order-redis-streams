package com.example.orders.redis;

import com.example.orders.model.OrderCreatedEvent;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
public class OrderEventJsonCodec {

    private final Jackson2JsonRedisSerializer<OrderCreatedEvent> serializer;

    public OrderEventJsonCodec(Jackson2JsonRedisSerializer<OrderCreatedEvent> serializer) {
        this.serializer = serializer;
    }

    public String encode(OrderCreatedEvent event) {

        byte[] bytes = serializer.serialize(event);


        return new String(bytes, StandardCharsets.UTF_8);
    }

    public OrderCreatedEvent decode(String payload) {
        byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);

        OrderCreatedEvent event = serializer.deserialize(bytes);
        if (event == null) {
            throw new IllegalStateException("Could not deserialize OrderCreatedEvent");
        }
        return event;
    }
}
