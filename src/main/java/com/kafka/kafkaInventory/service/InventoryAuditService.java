package com.kafka.kafkaInventory.service;

import com.kafka.kafkaInventory.model.InventoryAction;
import com.kafka.kafkaInventory.model.InventoryAudit;
import com.kafka.kafkaInventory.repository.InventoryAuditRepository;
import org.springframework.stereotype.Service;

@Service
public class InventoryAuditService {

    private final InventoryAuditRepository repository;

    public InventoryAuditService(InventoryAuditRepository repository){

        this.repository = repository;
    }

    public void recordChange(
            String productId,
            int delta,
            String source,
            InventoryAction action){

        InventoryAudit audit = new InventoryAudit(

                productId,
                delta,
                source,
                action,
                System.currentTimeMillis()
        );

        repository.save(audit);

    }
}
