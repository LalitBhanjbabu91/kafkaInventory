package com.kafka.kafkaInventory.service;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.kafkaInventory.dto.InventoryEvent;
import com.kafka.kafkaInventory.constants.KafkaConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class InventoryProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper mapper;

    private static final Logger logger = LoggerFactory.getLogger(InventoryProducer.class);

    public InventoryProducer(KafkaTemplate<String, String> kafkaTemplate,
                             ObjectMapper mapper) {

        this.kafkaTemplate = kafkaTemplate;
        this.mapper = mapper;
    }

    public void sendEvent(InventoryEvent event) {
        try {

            String json = mapper.writeValueAsString(event);
            kafkaTemplate.send(KafkaConstants.KAFKA_TOPIC , event.getProductId(), json);
            logger.info("Kafka event sent from producer for: {}", event.getProductId());

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
