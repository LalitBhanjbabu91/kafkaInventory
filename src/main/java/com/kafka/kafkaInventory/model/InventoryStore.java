package com.kafka.kafkaInventory.model;

import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class InventoryStore {

    private final ConcurrentHashMap<String, AtomicInteger> stock = new ConcurrentHashMap<>();

//    public void applyDelta(String productId, int delta) {
//        stock.computeIfAbsent(productId, k -> new AtomicInteger(0))
//                .addAndGet(delta);
//    }

    public int getStock(String productId) {

        return stock.getOrDefault(productId, new AtomicInteger(0)).get();
    }

    public Map<String, Integer> getAllStock() {
        Map<String, Integer> result = new HashMap<>();
        stock.forEach((key, value) -> result.put(key, value.get()));
        return result;
    }
}
