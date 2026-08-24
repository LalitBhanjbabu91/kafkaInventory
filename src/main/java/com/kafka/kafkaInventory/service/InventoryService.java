package com.kafka.kafkaInventory.service;

import com.kafka.kafkaInventory.dto.*;
import com.kafka.kafkaInventory.exception.InsufficientInventoryException;
import com.kafka.kafkaInventory.exception.InventoryNotFoundException;
import com.kafka.kafkaInventory.model.Inventory;
import com.kafka.kafkaInventory.repository.InventoryRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class InventoryService {

    private final InventoryRepository inventoryRepository;


    public InventoryService(InventoryRepository inventoryRepository){

        this.inventoryRepository = inventoryRepository;
    }

    public InventoryResponse getInventory(String productId){

        Inventory inventory = inventoryRepository.findByProductId(productId)
                .orElseThrow(() ->
                        new InventoryNotFoundException(
                                "Inventory not found for product: " + productId
                        ));

        return toResponse(inventory);

    }

    public List<InventoryResponse> getAllInventory(){

        return inventoryRepository.findAll()
                .stream()
                .map(this::toResponse)
                .toList();
    }

    /*
     * PUT semantics:
     * Replace the current quantity with an exact value.
     *
     * Example:
     * Current quantity = 100
     * Request quantity = 50
     * Final quantity = 50
     */
    @Transactional
    public InventoryResponse replaceInventory(String productId,
                                              InventoryReplaceRequest request){

        Inventory inventory = inventoryRepository.findByProductId(productId)
                .orElseThrow(() ->
                        new InventoryNotFoundException(

                                "Inventory not found for product: "+ productId

                        ));

        inventory.setQuantity(request.quantity());
        Inventory savedInventory = inventoryRepository.save(inventory);

        return toResponse(savedInventory);
    }

    /*
     * PATCH semantics:
     * Increment or decrement the current quantity.
     *
     * Example:
     * Current quantity = 100
     * delta = -10
     * Final quantity = 90
     */
    @Transactional
    public InventoryUpdateResponse adjustInventory(
            String productId,
            InventoryUpdateRequest request){

        Inventory inventory = inventoryRepository.findByProductId(productId)
                .orElseThrow(() ->
                        new InventoryNotFoundException(
                                "Inventory not found for product: "+ productId
                        ));

        int newQuantity =
                inventory.getQuantity() + request.delta();

        if (newQuantity < 0){

            throw new InsufficientInventoryException(
                    "Insufficient inventory for product: " + productId
            );
        }
        inventory.setQuantity(newQuantity);
        inventoryRepository.save(inventory);

        return new InventoryUpdateResponse(
                "updated",
                productId,
                request.delta(),
                newQuantity
        );

    }

    @Transactional
    public void deleteById(Long id){

        if (!inventoryRepository.existsById(id)){

            throw new InventoryNotFoundException(
                    "Inventory not found with id: " + id
            );
        }
        inventoryRepository.deleteById(id);
    }
    @Transactional
    public InventoryResponse createInventory(
            InventoryCreateRequest request) {

        Inventory inventory = new Inventory(request.productId(),
                request.quantity());

        Inventory savedInventory =
                inventoryRepository.save(inventory);

        return toResponse(savedInventory);
    }



    private InventoryResponse toResponse(
            Inventory inventory) {

        return new InventoryResponse(
                inventory.getId(),
                inventory.getProductId(),
                inventory.getQuantity()
        );
    }
}
