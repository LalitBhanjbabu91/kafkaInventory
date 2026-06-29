package com.kafka.kafkaInventory.service;


import com.kafka.kafkaInventory.dto.InventoryAudit;
import com.kafka.kafkaInventory.dto.InventoryEvent;
import com.kafka.kafkaInventory.repository.InventoryAuditRepository;
import com.kafka.kafkaInventory.repository.InventoryRepository;
import com.kafka.kafkaInventory.request.InventoryUpdateRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
@Transactional
public class InventoryService {

    private final InventoryRepository inventoryRepository;
    private final InventoryAuditRepository inventoryAuditRepository;
    private final InventoryProducer inventoryProducer;

    Logger logger = LoggerFactory.getLogger(InventoryService.class);

    public InventoryService(InventoryRepository inventoryRepository, InventoryAuditRepository inventoryAuditRepository, InventoryProducer inventoryProducer) {
        this.inventoryRepository = inventoryRepository;
        this.inventoryAuditRepository = inventoryAuditRepository;
        this.inventoryProducer = inventoryProducer;
    }

    public void updateStock(String productId, InventoryUpdateRequest request)
    {
        int rows = inventoryRepository.updateByProductId(productId, request.getDelta());

        if (rows > 0) {
            logger.info("Inventory updated for {}", productId);
        } else {
            logger.warn("Inventory NOT FOUND for {}", productId);
        }

        InventoryAudit inventoryAudit = new InventoryAudit();
        inventoryAudit.setProductId(productId);
        inventoryAudit.setDelta(request.getDelta());
        inventoryAudit.setReason(request.getReason());
        inventoryAudit.setSource(request.getSource());
        inventoryAudit.setTimestamp(request.getTimestamp());

        inventoryAuditRepository.save(inventoryAudit);
        logger.info("✅ saved Audit details to DB: " + productId);

        InventoryEvent event = new InventoryEvent(
                UUID.randomUUID().toString(),
                productId, request.getDelta(),
                System.currentTimeMillis());

        inventoryProducer.sendEvent(event);
        logger.info("✅ Updated inventory using PATCH for: " + productId);


    }
}
