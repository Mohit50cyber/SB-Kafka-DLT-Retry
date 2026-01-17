package com.Kafka_producer.service;

import com.Kafka_producer.dto.Customer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMessagePublisher {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    // Send simple String message
    public void sendMessageToTopic(String message) {

        CompletableFuture<SendResult<String, Object>> future =
                kafkaTemplate.send("topicmohit-1", message);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                System.out.println(
                        "Message sent to topic: " +
                                result.getRecordMetadata().topic() +
                                ", offset: " +
                                result.getRecordMetadata().offset()
                );
            } else {
                System.out.println("Failed to send message: " + ex.getMessage());
            }
        });
    }

    // Send JSON (Customer object)
    public void sendEventsToTopic(Customer customer) {

        CompletableFuture<SendResult<String, Object>> future =
                kafkaTemplate.send("topiccustomer", customer);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                System.out.println(
                        "Customer event sent: " + customer +
                                ", topic: " + result.getRecordMetadata().topic() +
                                ", offset: " + result.getRecordMetadata().offset()
                );
            } else {
                System.out.println("Failed to send customer event: " + ex.getMessage());
            }
        });
    }
}
