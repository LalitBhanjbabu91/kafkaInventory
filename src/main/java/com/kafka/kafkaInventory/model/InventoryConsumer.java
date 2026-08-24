package com.kafka.kafkaInventory.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.kafkaInventory.constants.KafkaConstants;
import com.kafka.kafkaInventory.dto.InventoryEvent;
import com.kafka.kafkaInventory.repository.InventoryRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class InventoryConsumer {

    private final InventoryRepository inventoryRepository;
    private final ObjectMapper mapper;
    private static final Logger logger = LoggerFactory.getLogger(InventoryConsumer.class);

    public InventoryConsumer(InventoryRepository inventoryRepository, ObjectMapper mapper) {
        this.inventoryRepository = inventoryRepository;
        this.mapper = mapper;
    }

    @KafkaListener(topics = KafkaConstants.KAFKA_TOPIC,
            groupId = KafkaConstants.KAFKA_GROUP_ID)
    public void consume(ConsumerRecord<String, String> record) {
        try {
            logger.info("Kafka Topic is -> "+KafkaConstants.KAFKA_TOPIC+
                    " Kafka group id is -> "+KafkaConstants.KAFKA_GROUP_ID);

            logger.info("Partition: {}, Offset: {}",
                    record.partition(),
                    record.offset());

            //System.out.println("📩 RAW MESSAGE FROM KAFKA: " + message);
            logger.info("Consumer = {}, Partition = {}, Offset = {}",
                    Thread.currentThread().getName(),
                    record.partition(),
                    record.offset());
            logger.info("CONSUMER_INSTANCE={} GROUP={} PARTITION={} OFFSET={}",
                    System.getProperty("server.port"),
                    KafkaConstants.KAFKA_GROUP_ID,
                    record.partition(),
                    record.offset());

            InventoryEvent event = mapper.readValue(record.value(), InventoryEvent.class);

            logger.info("Processing event: {} delta={}",
                    event.getProductId(),
                    event.getDelta());

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
            inventoryRepository.updateByProductId(
                    event.getProductId(), event.getDelta()
            );

            logger.info(
                    "Inventory updated for productId={}",
                    event.getProductId()
            );

        } catch (Exception e) {
            logger.info("Error processing Kafka message", e);

            e.printStackTrace();
        }
    }
}
