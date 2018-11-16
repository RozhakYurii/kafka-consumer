package com.rozhak.kafka.consumer.listener;

import com.rozhak.kafka.model.Message;
import com.rozhak.kafka.model.NotExactlyAMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;


@Service
@Slf4j
public class KafkaConsumer {

    @KafkaListener(topics = "${application.kafka.topicName}",
            groupId = "${application.kafka.groupId}", containerFactory = "kafkaListenerContainerFactory")
    public void consume(@Payload String message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
                        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                        @Header(KafkaHeaders.RECEIVED_PARTITION_ID) String partitionIid) {
        log.warn("New message \"{}\" consumed with key \"{}\" from topic \"{}\" partition \"{}\"  ", message, key, topic, partitionIid);
    }

    @KafkaListener(topics = "${application.kafka.jsonTopicName}", groupId = "${application.kafka.groupId.json}",
            containerFactory = "messageKafkaListenerContainerFactory")
    public void consumeJson(Message message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
                            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) String partitionIid) {
        log.warn("New json message \"{}\" consumed with key \"{}\" from topic \"{}\" partition \"{}\"", message, key, topic, partitionIid);
    }

    @KafkaListener(topics = {"${application.kafka.jsonTopicName}", "${application.kafka.genericJsonTopicName}"}, groupId = "groupNotExactlyMessage",
            containerFactory = "notExactlyMessageKafkaListenerContainerFactory")
    public void consumeJson(NotExactlyAMessage message,
                            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) String partitionIid) {
        log.warn("New json message \"{}\" consumed from topic \"{}\" partition \"{}\"", message, topic, partitionIid);
    }
}
