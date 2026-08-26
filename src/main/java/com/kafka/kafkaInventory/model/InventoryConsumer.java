package com.kafka.kafkaInventory.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.kafkaInventory.constants.KafkaConstants;
import com.kafka.kafkaInventory.dto.InventoryEvent;
import com.kafka.kafkaInventory.repository.InventoryRepository;
import com.kafka.kafkaInventory.service.InventoryAuditService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class InventoryConsumer {

    private final InventoryRepository inventoryRepository;
    private final ObjectMapper mapper;
    private final InventoryAuditService inventoryAuditService;
    private static final Logger logger = LoggerFactory.getLogger(InventoryConsumer.class);

    public InventoryConsumer(InventoryRepository inventoryRepository,
                             ObjectMapper mapper,
                             InventoryAuditService inventoryAuditService) {
        this.inventoryRepository = inventoryRepository;
        this.mapper = mapper;
        this.inventoryAuditService = inventoryAuditService;

    }

    @KafkaListener(topics = KafkaConstants.KAFKA_TOPIC,
            groupId = KafkaConstants.KAFKA_GROUP_ID)
    public void consume(ConsumerRecord<String, String> record) {
        try {

            InventoryEvent event = mapper.readValue(record.value(), InventoryEvent.class);

            logger.info(
                    "Consumed inventory event: productId={}, delta={}, partition={}, offset={}",
                    event.getProductId(),
                    event.getDelta(),
                    record.partition(),
                    record.offset()
            );

//            System.out.println("Processing event: " + event.getProductId()
//                    + " delta=" + event.getDelta());

            //store.applyDelta(event.getProductId(), event.getDelta());
            /*Inventory inventory = inventoryRepository.findByProductId(event.getProductId())
                            .orElse(new Inventory(0, event.getProductId(), 0));
            inventory.setQuantity(inventory.getQuantity() + event.getDelta());

            inventoryRepository.save(inventory);*/
            int rows = inventoryRepository.updateByProductId(
                    event.getProductId(), event.getDelta()
            );

            if (rows > 0){

                inventoryAuditService.recordChange(event.getProductId(),
                        event.getDelta(), "KAFKA", InventoryAction.ADJUST);

                logger.info(
                        "Inventory updated for productId={}",
                        event.getProductId() );

            }
            else{
                logger.warn(
                        "Inventory not found for productId={}",
                        event.getProductId());
            }

        } catch (Exception e) {

            logger.error("Error processing Kafka message", e);
        }
    }
}
