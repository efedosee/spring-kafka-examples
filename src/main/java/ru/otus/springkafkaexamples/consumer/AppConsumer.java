package ru.otus.springkafkaexamples.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import ru.otus.springkafkaexamples.dto.event.Event;
import ru.otus.springkafkaexamples.dto.message.Message;

@Component
@KafkaListener(topics = "${spring.kafka.template.default-topic}")
@Slf4j
public class AppConsumer {
    @KafkaHandler
    public void listen(@Payload Event event, Acknowledgment acknowledgment) {
        log.info("Fetched event: {}", event);
        acknowledgment.acknowledge();
    }

    @KafkaHandler
    public void listen(@Payload Message message, Acknowledgment acknowledgment) {
        log.info("Fetched message: {}", message);
        acknowledgment.acknowledge();
    }

//    @KafkaHandler(isDefault = true)
//    public void unknown(Object object) {
//        log.info("Fetched unknown: {}", object);
//    }
}
