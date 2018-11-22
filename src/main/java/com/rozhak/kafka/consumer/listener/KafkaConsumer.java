package com.rozhak.kafka.consumer.listener;

import com.rozhak.kafka.model.Message;
import com.rozhak.kafka.model.NotExactlyAMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;


@Service
@Slf4j
public class KafkaConsumer {

    @KafkaListener(topics = {"${application.kafka.topicName}", "${application.kafka.streamTopicName}"},
            groupId = "${application.kafka.groupId}", containerFactory = "kafkaListenerContainerFactory")
    public void consume(String message,
                        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                        @Header(KafkaHeaders.RECEIVED_PARTITION_ID) String partitionIid) {
        log.info("New message \"{}\" consumed from topic \"{}\" partition \"{}\"  ",
                message, topic, partitionIid);
    }

    @KafkaListener(topics = {"${application.kafka.jsonTopicName}", "${application.kafka.streamTopicName}",
            "${application.kafka.genericJsonTopicName}"},
            groupId = "${application.kafka.groupId.json.message}",
            containerFactory = "typeMapperListenerContainerFactory")
    public void consumeJson(Message message,
                            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) String partitionIid) {
        log.info("New json message \"{}\" consumed as object of class \"{}\" from topic \"{}\" partition \"{}\"",
                message, message.getClass().getName(), topic, partitionIid);
    }

    @KafkaListener(topics = {"${application.kafka.genericJsonTopicName}", "${application.kafka.streamTopicName}"}, groupId = "${application.kafka.groupId.json.notexatlyamessage}",
            containerFactory = "typeMapperListenerContainerFactory")
    public void consumeJson(NotExactlyAMessage message,
                            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) String partitionIid) {
        log.info("New json message \"{}\" consumed as object of class \"{}\" from topic \"{}\" partition \"{}\"",
                message, message.getClass().getName(), topic, partitionIid);
    }

    @KafkaListener(topics = {"${application.kafka.jsonTopicName}", "${application.kafka.streamTopicName}",
            "${application.kafka.genericJsonTopicName}"},
            groupId = "${application.kafka.groupId.json.nospecifiedclass.message}",
            containerFactory = "noTypeMapperListenerContainerFactory")
    public void consumeJsonWithoutClassSpecified(Message message,
                            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) String partitionIid) {
        log.info("NoSpecifiedClassConsumerFactory: New json message \"{}\" consumed as object of class \"{}\" from topic \"{}\" partition \"{}\"",
                message, message.getClass().getName(), topic, partitionIid);
    }

    @KafkaListener(topics = {"${application.kafka.genericJsonTopicName}", "${application.kafka.streamTopicName}",
            "${application.kafka.jsonTopicName}"},
            groupId = "${application.kafka.groupId.json.nospecifiedclass.notexatlyamessage}",
            containerFactory = "noTypeMapperListenerContainerFactory")
    public void consumeJsonWithoutClassSpecified(NotExactlyAMessage message,
                            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) String partitionIid) {
        log.info("NoSpecifiedClassConsumerFactory: New json message \"{}\" consumed as object of class \"{}\" from topic \"{}\" partition \"{}\" ",
                message, message.getClass().getName(), topic, partitionIid);
    }

    @Component
    @KafkaListener(id = "Kafka2.0Group", containerFactory = "typeMapperListenerContainerFactory",
            topics = {"${application.kafka.jsonTopicName}", "${application.kafka.genericJsonTopicName}"})
    public static class KafkaListenerWithSpecificHandlers {


        @KafkaHandler
        public void consumeMessage(@Payload Message message,
                                   @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                   @Header(KafkaHeaders.RECEIVED_PARTITION_ID) String partitionIid) {
            log.info("KAFKA New json message \"{}\" consumed as object of class \"{}\" from topic \"{}\" partition \"{}\"",
                    message, message.getClass().getName(), topic, partitionIid);
        }

        @KafkaHandler
        public void consumeNotExactlyAMessage(@Payload NotExactlyAMessage notExactlyAMessage,
                                              @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                              @Header(KafkaHeaders.RECEIVED_PARTITION_ID) String partitionIid) {
            log.info("KAFKA New json message \"{}\" consumed as object of class \"{}\" from topic \"{}\" partition \"{}\"",
                    notExactlyAMessage, notExactlyAMessage.getClass().getName(), topic, partitionIid);
        }
    }
}
