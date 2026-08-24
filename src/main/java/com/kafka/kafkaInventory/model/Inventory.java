package com.kafka.kafkaInventory.model;


import jakarta.persistence.*;
import lombok.*;

@Entity
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@Getter
@Setter
@Table(name = "inventory")
public class Inventory {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;

    @Column(unique = true)
    private String productId;

    private int quantity;

    public Inventory(String productId, int quantity){

        this.productId = productId;
        this.quantity = quantity;
    }

}
