package com.Kafka_producer.controller;

import com.Kafka_producer.dto.Customer;
import com.Kafka_producer.dto.User;
import com.Kafka_producer.service.KafkaMessagePublisher;
import com.Kafka_producer.service.PublisherDLT;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/producer")
public class EventController {

    @Autowired
    private KafkaMessagePublisher publisher;

    @Autowired
    private PublisherDLT publisherDLT;

    @GetMapping("/publish/{message}")
    public ResponseEntity<?> publishmessage(@PathVariable String message){
        try{
            for(int i=0;i<10000;i++){
                publisher.sendMessageToTopic(message + " " + i); // Appending index to message for uniqueness
            }

            return ResponseEntity.ok("Message published successfully....");
        } catch (Exception ex) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .build();
        }
    }

    @PostMapping("/publish")
    public void sendEvents(@RequestBody Customer customer){
        publisher.sendEventsToTopic(customer);
    }

    @PostMapping("/publishnew")
    public ResponseEntity<?> publishEvents(@RequestBody User user){
        try{
            publisherDLT.sendEvents(user);
            return ResponseEntity.ok("User event published successfully....");
        } catch (Exception ex) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .build();
        }
    }
}
