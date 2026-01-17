package com.Kafka_consumer.service;

import com.Kafka_consumer.dto.Customer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {

    private static final Logger logger =
            LoggerFactory.getLogger(KafkaConsumerService.class);

    // ✅ Constructor ONLY for initialization
    public KafkaConsumerService() {
        logger.info("KafkaConsumerService initialized");
    }
    @KafkaListener(
            topics = "topiccustomer",
            groupId = "qwerty"
    )
    public void consumeEvents(Customer customer) {
        logger.info("Consumed message: 1:  {}", customer.toString());
    }


    // ✅ Kafka listener MUST be a method
    @KafkaListener(
            topics = "topicmohit-1",
            groupId = "qwerty"
    )
    public void consume1(String message) {
        logger.info("Consumed message: 1:  {}", message);
    }

    @KafkaListener(
            topics = "topicmohit-1",
            groupId = "qwerty"
    )
    public void consume2(String message) {
        logger.info("Consumed message: 2:  {}", message);
    }

    @KafkaListener(
            topics = "topicmohit-1",
            groupId = "qwerty"
    )
    public void consume3(String message) {
        logger.info("Consumed message: 3 :  {}", message);
    }

    @KafkaListener(
            topics = "topicmohit-1",
            groupId = "qwerty"
    )
    public void consume4(String message) {
        logger.info("Consumed message: 4: {}", message);
    }


}
