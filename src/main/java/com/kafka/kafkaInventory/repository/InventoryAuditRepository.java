package com.kafka.kafkaInventory.repository;

import com.kafka.kafkaInventory.dto.InventoryAudit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface InventoryAuditRepository extends JpaRepository<InventoryAudit, Long>{
}
