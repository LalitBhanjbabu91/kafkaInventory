package com.kafka.kafkaInventory.model;

import jakarta.persistence.*;
import lombok.*;


@Entity
@Table(name = "inventory_audit")
@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class InventoryAudit {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String productId;
    private int delta;
    private String source;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)

    private InventoryAction action;
    private long timestamp;

    public InventoryAudit(
            String productId,
            int delta,
            String source,
            InventoryAction action,
            long timestamp){
        this.productId = productId;
        this.delta = delta;
        this.source = source;
        this.action = action;
        this.timestamp = timestamp;
    }
}
