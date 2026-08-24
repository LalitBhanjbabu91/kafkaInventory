package com.kafka.kafkaInventory.dto;

public record InventoryResponse(
        Long id,
        String productId,
        int quantity
) {
}
