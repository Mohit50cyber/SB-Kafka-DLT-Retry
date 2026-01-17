package com.Kafka_consumer.service;

import com.Kafka_consumer.dto.User;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Stream;

@Service
@Slf4j
public class ConsumerDLT {

    @RetryableTopic(attempts = "4",
            exclude = {NullPointerException.class, RuntimeException.class}
            ,backOff = @BackOff(delay = 3000,multiplier = 1.5,maxDelay = 15000)) // 3 (n-1)
    @KafkaListener(topics = "DLT", groupId = "qwerty")
    public void consumeEvents(User user, @Header (KafkaHeaders.RECEIVED_TOPIC) String topic,
                              @Header (KafkaHeaders.OFFSET) long offset) {

        try{
            log.info("Received message from topic: {}, " +
                    " offset: {}, user: {}", new ObjectMapper().writeValueAsString(user),
                    topic, offset);

            List<String> restrictedIPList= Stream.of(
                    "34.677.235.23","78.234.123.678","123.456.789.12","34.23.12.78"
            ).toList();
            if(restrictedIPList.contains(user.getIpAddress())){
                throw new RuntimeException("Invalid IP Address !!!!!");
            }
        }catch (JsonProcessingException e){
            e.printStackTrace();
        }
    }

    @DltHandler
    public void listenDLT(User user, @Header (KafkaHeaders.RECEIVED_TOPIC) String topic,
                          @Header (KafkaHeaders.OFFSET) long offset){
        log.info("DLT Listener - Received message from topic: {}, " +
                " offset: {}, user: {}",user.getFirstName(), topic, offset);
    }
}
