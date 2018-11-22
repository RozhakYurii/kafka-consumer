package com.rozhak.kafka.streams;

import com.rozhak.kafka.model.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Configuration
@EnableKafkaStreams
@Slf4j
public class KafkaStream {

    @Value("${application.kafka.bootstrapserver.url}")
    private String bootstrapServerUrl;
    @Value("${application.kafka.bootstrapserver.port}")
    private String bootstrapServerPort;
    @Value("${application.kafka.streamTopicName}")
    private String outputTopicName;
    @Value("${application.kafka.topicName}")
    private String inputTopicName;

    @Autowired
    StringJsonMessageConverter advancedMessageConverter;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration config() {
        Map<String, Object> config = new HashMap<>();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "Kafka-streams");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServerUrl + ":" + bootstrapServerPort);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        // was changed only for debug to faster emulate strange behavior
        //        config.put(StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG, 6000);
        config.put(StreamsConfig.STATE_DIR_CONFIG, "D:\\DevSoftware\\Kafka\\kafka_2.11-2.0.0\\kafka-streams");

        return new KafkaStreamsConfiguration(config);
    }


    //If not using @EnableKafkaStreams annotation we need to create streams consumer factory bean manually
/*@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_BUILDER_BEAN_NAME)
public StreamsBuilderFactoryBean factoryBean() {
return new StreamsBuilderFactoryBean(config());
}
*/

    @Bean
    public KStream<String, String> kStream(StreamsBuilder builder) {

        KStream<String, String> kStreamIn = builder.stream(inputTopicName, Consumed.with(Serdes.String(), Serdes.String()));
        kStreamIn.mapValues((ValueMapper<String, String>) String::toLowerCase)
                .selectKey((key, value) -> UUID.randomUUID().toString())
                .peek((key, value) -> log.info("B4 MAPPING VALUE \"{}\" with key \"{} \"  was processed", value, key))
                .mapValues(value -> new Message(value + " processed by stream", "defaultTag"))
                .peek((key, value) -> log.info("AFTER MAPPING VALUE \"{}\" with key \"{} \"  was processed", value, key))
                .to(outputTopicName, Produced.with(Serdes.String(), new JsonSerde<>()));

        return kStreamIn;
    }
}
