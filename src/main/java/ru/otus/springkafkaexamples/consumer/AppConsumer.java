package ru.otus.springkafkaexamples.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import ru.otus.springkafkaexamples.dto.event.Event;

@Component
@Slf4j
public class AppConsumer {
    /*
    The DefaultErrorHandler considers certain exceptions to be fatal, and retries are skipped for such exceptions;
    the recoverer is invoked on the first failure.
    The exceptions that are considered fatal, by default, are:
        DeserializationException
        MessageConversionException
        ConversionException
        MethodArgumentResolutionException
        NoSuchMethodException
        ClassCastException
     */
    @RetryableTopic
    @KafkaListener(topics = "${spring.kafka.template.default-topic}")
    public void listen(@Payload Event event, Acknowledgment acknowledgment) {
        log.info("Fetched event: {}", event);
        acknowledgment.acknowledge();
//        throw new RuntimeException("retry test");
    }
}
