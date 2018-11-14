package com.rozhak.kafka.producer.controller;

import com.rozhak.kafka.model.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("message")
public class MessagePublisher {


    private final KafkaTemplate<String, String> regularKafkaTemplate;
    private final KafkaTemplate<String, Message> messageKafkaTemplate;

    @Autowired
    public MessagePublisher(KafkaTemplate<String, String> regularKafkaTemplate, KafkaTemplate<String, Message> messageKafkaTemplate) {
        this.regularKafkaTemplate = regularKafkaTemplate;
        this.messageKafkaTemplate = messageKafkaTemplate;
    }

    @Value("${application.kafka.jsonTopicName}")
    private String messageTopicName;

    @Value("${application.kafka.topicName}")
    private String stringTopicName;

    @GetMapping("/publish/{message}")
    public String publishRegularMessageToKafka(@PathVariable("message") String message) {
        regularKafkaTemplate.send(stringTopicName, message);
        return "Message was successfully published to Kafka:" + message;
    }

    @GetMapping("/publishAsJson/{message}")
    public String publishJsonMessageToKafka(@PathVariable("message") String message) {
        messageKafkaTemplate.send(messageTopicName, new Message(message, "defaultTag"));
        return "JSON Message was successfully published to Kafka:" + message;
    }

    @PostMapping(value = "/publishAsJson")
    public String publishJsonMessageToKafka(@RequestBody Message message) {
        messageKafkaTemplate.send(messageTopicName, message);
        return "JSON Message was successfully published to Kafka:" + message;
    }
}
