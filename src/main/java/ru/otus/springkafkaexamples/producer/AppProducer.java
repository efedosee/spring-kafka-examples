package ru.otus.springkafkaexamples.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import ru.otus.springkafkaexamples.dto.event.Event;
import ru.otus.springkafkaexamples.dto.event.EventType;
import ru.otus.springkafkaexamples.dto.event.Payload;

import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
public class AppProducer {
    private final KafkaTemplate<String, Event> eventKafkaTemplate;


    @Scheduled(fixedRate = 10_000)
    public void sendEvent() {
        UUID id = UUID.randomUUID();
        var event = new Event(
                id,
                EventType.TEST_EVENT,
                new Payload("Hello")
        );
        eventKafkaTemplate.sendDefault(id.toString(), event)
                .exceptionally(ex -> {
                    log.error("Exception while sending event", ex);
                    return null;
                })
                .thenAccept(res -> log.info("Sent event result: {}", res));
    }
}
