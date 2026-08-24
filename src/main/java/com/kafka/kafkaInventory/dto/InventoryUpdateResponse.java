package com.kafka.kafkaInventory.dto;

public record InventoryUpdateResponse(

        String status,
        String productId,
        int delta,
        int quantity
) {
}
