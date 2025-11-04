package ru.otus.springkafkaexamples.configuration;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@RequiredArgsConstructor
public class AppConfiguration {
    private final KafkaProperties kafkaProperties;

    @Bean
    public NewTopic testTopic1() {
        return TopicBuilder.name(kafkaProperties.getTemplate().getDefaultTopic())
                .partitions(2)
                .build();
    }
}
