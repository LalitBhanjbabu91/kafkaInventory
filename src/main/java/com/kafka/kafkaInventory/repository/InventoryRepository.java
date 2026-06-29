package com.kafka.kafkaInventory.repository;

import com.kafka.kafkaInventory.model.Inventory;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;


@Repository
public interface InventoryRepository extends JpaRepository<Inventory, Long> {

    Optional<Inventory> findByProductId(String productId);

    @Transactional
    @Modifying
    @Query("UPDATE Inventory i SET i.quantity = i.quantity + :delta WHERE i.productId = :productId")
    int updateByProductId(@Param("productId") String productId,
                          @Param("delta") int delta);

}
