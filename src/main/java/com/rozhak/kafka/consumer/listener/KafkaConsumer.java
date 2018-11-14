package com.rozhak.kafka.consumer.listener;

import com.rozhak.kafka.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;


@Service
public class KafkaConsumer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);


    @KafkaListener(topics = {"${application.kafka.topicName}", "${application.kafka.jsonTopicName}"},
            groupId = "${application.kafka.groupId}", containerFactory = "kafkaListenerContainerFactory")
    public void consume(String message) {
        logger.info("New message consumed: {}", message);
    }

    @KafkaListener(topics = "${application.kafka.jsonTopicName}", groupId = "${application.kafka.groupId.json}",
            containerFactory = "messageKafkaListenerContainerFactory")
    public void consumeJson(Message message) {
        logger.info("New json message consumed: {}", message);
    }
}
