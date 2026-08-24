package com.kafka.kafkaInventory.controller;


import com.kafka.kafkaInventory.dto.*;
import com.kafka.kafkaInventory.service.InventoryProducer;
import com.kafka.kafkaInventory.service.InventoryService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/inventory")
public class InventoryController {

    private final InventoryProducer inventoryProducer;
    private final InventoryService inventoryService;

    public InventoryController(InventoryProducer inventoryProducer,
                               InventoryService inventoryService){

        this.inventoryProducer = inventoryProducer;
        this.inventoryService = inventoryService;

    }

    // Publish inventory event to Kafka
    @PostMapping("/event")
    public ResponseEntity<InventoryEvent> publishEvent(@RequestBody InventoryEvent event){


        if (event.getEventId() == null){
            event = new InventoryEvent(
                    UUID.randomUUID().toString(),
                    event.getProductId(),
                    event.getDelta(),
                    System.currentTimeMillis()
            );
        }

        inventoryProducer.sendEvent(event);
        return ResponseEntity.accepted().body(event);
    }

    // Get inventory by productId
    @GetMapping("/{productId}")
    public ResponseEntity<InventoryResponse> getInventory(@PathVariable String productId){


        InventoryResponse response =
                inventoryService.getInventory(productId);

        return ResponseEntity.ok(response);
    }

    // Get all inventory
    @GetMapping
    public ResponseEntity<List<InventoryResponse>> getAllInventory(){

        return ResponseEntity.ok(
                inventoryService.getAllInventory()
        );

    }
    // PUT = set quantity to an exact value
    @PutMapping("/{productId}")
    public ResponseEntity<InventoryResponse> replaceInventory(
            @PathVariable String productId,
            @Valid @RequestBody InventoryReplaceRequest request) {

        InventoryResponse response =
                inventoryService.replaceInventory(
                        productId,
                        request
                );

        return ResponseEntity.ok(response);
    }

    // PATCH = increment/decrement existing quantity
    @PatchMapping("/{productId}")
    public ResponseEntity<InventoryUpdateResponse> adjustInventory(
            @PathVariable String productId,
            @RequestBody InventoryUpdateRequest request) {

        InventoryUpdateResponse response =
                inventoryService.adjustInventory(
                        productId,
                        request
                );

        return ResponseEntity.ok(response);
    }

    @PostMapping
    public ResponseEntity<InventoryResponse> createInventory(
            @Valid @RequestBody InventoryCreateRequest request) {

        InventoryResponse response =
                inventoryService.createInventory(request);

        return ResponseEntity
                .status(HttpStatus.CREATED)
                .body(response);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteInventory(@PathVariable Long id){

        inventoryService.deleteById(id);

        return ResponseEntity.noContent().build();
    }

}
