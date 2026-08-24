package com.kafka.kafkaInventory.exception;

public class InsufficientInventoryException extends RuntimeException{

    public InsufficientInventoryException(String message){

        super(message);
    }
}
