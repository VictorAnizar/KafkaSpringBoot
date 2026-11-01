package com.email.emailnotification.handler;

import kafka.repaso.kafka.domain.events.ProductCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@KafkaListener(topics = "products-created-events-topic")
public class ProductCreatedEventHandler {

    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    //If the anotation is atached to a method, it means that whenever a message is recieved, this method will be invoked
    //if were multiple topics: @KafkaListener(topics = {"topic1", "topic2"})
    //If needed to handle different types in the same class, is better to put the annotation in the class instead of a method
    //and if that´s the case, the method should have @KafkaHandler annotation
    //@KafkaListener(topics = "products-created-events-topic")
    @KafkaHandler
    public void handle(ProductCreatedEvent productCreatedEvent){
        LOGGER.info("Recieved a new event: "+productCreatedEvent.getTitle());
    }
}
