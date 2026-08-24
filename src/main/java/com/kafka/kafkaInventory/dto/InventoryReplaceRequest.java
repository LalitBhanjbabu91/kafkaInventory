package com.kafka.kafkaInventory.dto;

import jakarta.validation.constraints.PositiveOrZero;

public record InventoryReplaceRequest(

        @PositiveOrZero(message = "Quantity cannot be negative")
        int quantity
) {
}
