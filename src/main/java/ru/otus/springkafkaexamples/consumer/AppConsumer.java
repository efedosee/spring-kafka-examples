package ru.otus.springkafkaexamples.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class AppConsumer {
    @KafkaListener(topics = "test-topic-3")
    public void listen(@Payload String data) {
        log.info("Fetched string: {}", data);
    }
}
