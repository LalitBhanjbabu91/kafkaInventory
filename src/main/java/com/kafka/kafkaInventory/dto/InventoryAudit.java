package com.kafka.kafkaInventory.dto;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "inventory_audit")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class InventoryAudit {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String productId;
    private int delta;
    private String reason;
    private String source;
    private long timestamp;
}