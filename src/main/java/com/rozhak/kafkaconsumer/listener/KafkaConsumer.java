package com.rozhak.kafkaconsumer.listener;

import com.rozhak.kafkaconsumer.model.Message;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {

//    @Value("${application.kafka.topicName}")
//    private String topicName;


    @KafkaListener(topics = "${application.kafka.topicName}", groupId = "${application.kafka.groupId}")
    public void consume(String message) {
        System.out.println("New message consumed: " + message);
    }

    @KafkaListener(topics = "${application.kafka.topicName}", groupId = "${application.kafka.groupId.json}", containerFactory = "messageKafkaListenerContainerFactory")
    public void consumeJson(Message message) {
        System.out.println("New json message consumed: " + message);
    }
}
