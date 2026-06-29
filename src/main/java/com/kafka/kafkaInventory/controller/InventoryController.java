package com.kafka.kafkaInventory.controller;

import com.kafka.kafkaInventory.dto.InventoryEvent;
import com.kafka.kafkaInventory.model.InventoryStore;
import com.kafka.kafkaInventory.request.InventoryUpdateRequest;
import com.kafka.kafkaInventory.repository.InventoryRepository;
import com.kafka.kafkaInventory.service.InventoryService;
import com.kafka.kafkaInventory.service.InventoryProducer;
import org.springframework.web.bind.annotation.*;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/inventory")
public class InventoryController {

    private final InventoryRepository inventoryRepository;
    private final InventoryProducer producer;
    private final InventoryStore store;
    private final InventoryService inventoryService;


    public InventoryController(InventoryRepository inventoryRepository, InventoryProducer producer, InventoryStore store, InventoryService inventoryService) {
        this.inventoryRepository = inventoryRepository;
        this.producer = producer;
        this.store = store;
        this.inventoryService = inventoryService;
    }

    // Send event to Kafka
    @PostMapping("/event")
    public Map<String, String> publish(@RequestBody InventoryEvent event) {

        // ensure eventId exists
        if (event.getEventId() == null) {
            event = new InventoryEvent(
                    UUID.randomUUID().toString(),
                    event.getProductId(),
                    event.getDelta(),
                    System.currentTimeMillis()
            );
        }

        producer.sendEvent(event);

        return Map.of(
                "status", "sent",
                "eventId", event.getEventId()
        );
    }

    // Get inventory
    @GetMapping("/{productId}")
    public Map<String, Object> getStock(@PathVariable String productId) {
        return Map.of(
                "productId", productId,
                "stock", store.getStock(productId)
        );

    }
    @GetMapping("/all")
    public Map<String, Object> getAll() {
        return Map.of("stocks", inventoryRepository.findAll());
    }

    @DeleteMapping("/{id}")
    public Map<String, Object> deleteById(@PathVariable  Long id){

        inventoryRepository.deleteById(id);
        return Map.of("status", "deleted", "id", String.valueOf(id));
    }

    @PutMapping("/{id}/{delta}")
    public Map<String, Object> updateById(@PathVariable String id, @PathVariable int delta)
    {
        int rows = inventoryRepository.updateByProductId(id, delta);

        return Map.of(
                "status", rows > 0 ? "updated" : "not_found",
                "rowsAffected", rows,
                "id", id
        );
    }
    // ✅ PATCH using JSON BODY (NEW CLEAN WAY)
    @PatchMapping("/{productId}")
    public Map<String, Object> updateStock(@PathVariable String productId,
                                           @RequestBody InventoryUpdateRequest request) {

        inventoryService.updateStock(productId, request);

        return Map.of(
                "status", "updated",
                "productId", productId,
                "delta", request.getDelta(),
                "reason", request.getReason(),
                "source", request.getSource(),
                "timestamp", request.getTimestamp()
        );



    }

}
