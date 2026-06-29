package com.kafka.kafkaInventory.dto;


import lombok.*;

@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor
@Getter
@Setter
public class InventoryEvent {

    private String eventId;
    private String productId;
    private int delta;
    private long timestamp;

}
