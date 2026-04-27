package com.example.orders.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

import java.math.BigDecimal;

public record CreateOrderRequest(
        @NotBlank(message = "userId must not be blank")
        String userId,

        @NotBlank(message = "productCode must not be blank")
        String productCode,

        @Min(value = 1, message = "quantity must be greater than 0")
        int quantity,

        @DecimalMin(value = "0.0", inclusive = false, message = "totalPrice must be greater than 0")
        BigDecimal totalPrice
) {
}
