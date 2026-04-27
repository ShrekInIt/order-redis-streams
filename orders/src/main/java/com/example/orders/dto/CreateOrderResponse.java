package com.example.orders.dto;

import java.util.UUID;

public record CreateOrderResponse(
        UUID orderId,
        String status
) {
}
