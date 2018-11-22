package com.rozhak.kafka.config;

import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

@Configuration
@EnableKafka
public class ProducerConfigurations {

    @Value("${application.kafka.bootstrapserver.url}")
    private String bootstrapServerUrl;
    @Value("${application.kafka.bootstrapserver.port}")
    private String bootstrapServerPort;


    @Autowired
    RecordMessageConverter messageConverter;

    @Bean
    public ProducerFactory<String, String> regularProducerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServerUrl + ":" + bootstrapServerPort);
        config.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, String> regularKafkaTemplate() {
        final KafkaTemplate<String, String> regularKafkaTemplate =
                defaultKafkaTemplate();
// new KafkaTemplate<>(regularProducerFactory());

        regularKafkaTemplate.setDefaultTopic("test");
        return regularKafkaTemplate;
    }

    @Bean
    public KafkaTemplate<String, String> messageKafkaTemplate() {
        final KafkaTemplate<String, String> notExactlyAMessageKafkaTemplate = defaultKafkaTemplate();
        notExactlyAMessageKafkaTemplate.setDefaultTopic("testJson");
        return notExactlyAMessageKafkaTemplate;
    }

    @Bean
    public KafkaTemplate<String, String> notExactlyAMessageKafkaTemplate() {
        final KafkaTemplate<String, String> notExactlyAMessageKafkaTemplate = new KafkaTemplate<>(regularProducerFactory());
        notExactlyAMessageKafkaTemplate.setMessageConverter(new StringJsonMessageConverter());
        notExactlyAMessageKafkaTemplate.setDefaultTopic("testJsonButNotExactlyMessage");
        return notExactlyAMessageKafkaTemplate;
    }

    private KafkaTemplate<String, String> defaultKafkaTemplate() {
        final KafkaTemplate<String, String> notExactlyAMessageKafkaTemplate = new KafkaTemplate<>(regularProducerFactory());
        notExactlyAMessageKafkaTemplate.setMessageConverter(messageConverter);
        return notExactlyAMessageKafkaTemplate;
    }
}
