package com.rozhak.kafka.consumer.listener;

import com.rozhak.kafka.model.Message;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;


@Service
@Slf4j
public class KafkaConsumer {

    @KafkaListener(topics = "${application.kafka.topicName}",
            groupId = "${application.kafka.groupId}", containerFactory = "kafkaListenerContainerFactory")
    public void consume(String message) {
        log.warn("New message consumed: {}", message);
    }

    @KafkaListener(topics = "${application.kafka.jsonTopicName}", groupId = "${application.kafka.groupId.json}",
            containerFactory = "messageKafkaListenerContainerFactory")
    public void consumeJson(Message message) {
        log.warn("New json message consumed: {}", message);
    }
}
