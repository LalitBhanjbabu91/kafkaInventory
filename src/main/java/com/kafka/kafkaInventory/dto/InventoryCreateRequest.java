package com.kafka.kafkaInventory.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.PositiveOrZero;

public record InventoryCreateRequest(

        @NotBlank(message = "Product ID is required")
        String productId,

        @PositiveOrZero(message = "Quantity cannot be negative")
        int quantity
) {
}
