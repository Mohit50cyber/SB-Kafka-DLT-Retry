package com.Kafka_producer.service;

import com.Kafka_producer.dto.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class PublisherDLT {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplates;

    public void sendEvents(User user) {

        CompletableFuture<SendResult<String, Object>> future =
                kafkaTemplates.send("DLT", user);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                System.out.println(
                        "Message sent to topic: " + user.toString() +
                                result.getRecordMetadata().topic() +
                                ", offset: " +
                                result.getRecordMetadata().offset()
                );
            } else {
                System.out.println("Failed to send message: " + ex.getMessage());
            }
        });
    }
}
