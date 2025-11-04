package ru.otus.springkafkaexamples.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import ru.otus.springkafkaexamples.dto.event.Event;

@Component
@Slf4j
public class AppConsumer {
    @KafkaListener(topics = "${spring.kafka.template.default-topic}")
    public void listen(@Payload Event event, Acknowledgment acknowledgment) {
        log.info("Fetched event: {}", event);
        acknowledgment.acknowledge();
    }

    @KafkaListener(topics = "test-topic-1")
    public void listen(Object data) {
        log.info("Fetched data: {}", data);
    }
}
