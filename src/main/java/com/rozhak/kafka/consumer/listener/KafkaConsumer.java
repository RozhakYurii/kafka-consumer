package com.rozhak.kafka.consumer.listener;

import com.rozhak.kafka.model.Message;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {

//    @Value("${application.kafka.topicName}")
//    private String topicName;


    @KafkaListener(topics = {"${application.kafka.topicName}", "${application.kafka.jsonTopicName}"},
            groupId = "${application.kafka.groupId}", containerFactory = "kafkaListenerContainerFactory")
    public void consume(String message) {
        System.out.println("New message consumed: " + message);
    }

    @KafkaListener(topics = "${application.kafka.jsonTopicName}", groupId = "${application.kafka.groupId.json}",
            containerFactory = "messageKafkaListenerContainerFactory")
    public void consumeJson(Message message) {
        System.out.println("New json message consumed: " + message);
    }
}
