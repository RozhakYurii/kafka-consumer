package com.rozhak.kafka.producer.controller;

import com.rozhak.kafka.model.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("message")
public class MessagePublisher {

    @Autowired
    KafkaTemplate<String, String> regularKafkaTemplate;

    @Autowired
    KafkaTemplate<String, Message> messageKafkaTemplate;

    @Value("${application.kafka.jsonTopicName}")
    private String MESSAGE_TOPIC_NAME;

    @Value("${application.kafka.topicName}")
    private String STRING_TOPIC_NAME;

    @GetMapping("/publish/{message}")
    public String publishRegularMessageToKafka(@PathVariable("message") String message) {
        regularKafkaTemplate.send(STRING_TOPIC_NAME, message);
        return "Message was successfully published to Kafka:" + message;
    }

    @GetMapping("/publishAsJson/{message}")
    public String publishJsonMessageToKafka(@PathVariable("message") String message) {
        messageKafkaTemplate.send(MESSAGE_TOPIC_NAME, new Message(message, "defaultTag"));
//        messageKafkaTemplate.send(STRING_TOPIC_NAME, new Message(message, "defaultTag"));
        return "JSON Message was successfully published to Kafka:" + message;
    }

    @PostMapping(value = "/publishAsJson")
    public String publishJsonMessageToKafka(@RequestBody Message message) {
        messageKafkaTemplate.send(MESSAGE_TOPIC_NAME, message);
//        messageKafkaTemplate.send(STRING_TOPIC_NAME, message);
        return "JSON Message was successfully published to Kafka:" + message;
    }
}
