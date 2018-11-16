package com.rozhak.kafka.producer.controller;

import com.rozhak.kafka.model.Message;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("message")
@Slf4j
public class MessagePublisher {

    @Value("${application.kafka.jsonTopicName}")
    private String messageTopicName;

    @Value("${application.kafka.topicName}")
    private String stringTopicName;

    private final KafkaTemplate<String, String> regularKafkaTemplate;
    private final KafkaTemplate<String, Message> messageKafkaTemplate;

    @Autowired
    public MessagePublisher(KafkaTemplate<String, String> regularKafkaTemplate, KafkaTemplate<String, Message> messageKafkaTemplate) {
        this.regularKafkaTemplate = regularKafkaTemplate;
        this.messageKafkaTemplate = messageKafkaTemplate;
    }

    @GetMapping("/publish/{message}")
    public String publishRegularMessageToKafka(@PathVariable("message") String message) {
        final String messageKey = UUID.randomUUID().toString();
        regularKafkaTemplate.send(stringTopicName, messageKey, message);
        return "Message was successfully published to Kafka:" + message;
    }

    @GetMapping("/publishAsJson/{message}")
    public String publishJsonMessageToKafka(@PathVariable("message") String message) {
        final Message messageObject = new Message(message, "defaultTag");
        final String messageKey = UUID.randomUUID().toString();
        messageKafkaTemplate.send(messageTopicName, messageKey, messageObject);
        log.info("JSON Message was successfully published to Kafka: {}", message);
        return "JSON Message was successfully published to Kafka:" + message;
    }

    @PostMapping(value = "/publishAsJson")
    public String publishJsonMessageToKafka(@RequestBody Message message) {
        final String messageKey = UUID.randomUUID().toString();
        messageKafkaTemplate.send(messageTopicName, messageKey, message);
        log.info("JSON Message was successfully published to Kafka: {}", message);
        return "JSON Message was successfully published to Kafka:" + message;
    }
}
