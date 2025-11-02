package ru.otus.springkafkaexamples.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Random;

@Component
@RequiredArgsConstructor
@Slf4j
public class AppProducer {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final static String topic = "test-topic-1";
    private final static Random random = new Random();


    @Scheduled(fixedRate = 1_000)
    public void sendMessage() {
        String randomValue = String.valueOf(random.nextInt(1000));
        kafkaTemplate.send(topic, randomValue, "test-".concat(randomValue))
                .exceptionally(ex -> {
                    log.error("Exception while sending message", ex);
                    return null;
                })
                .thenAccept(res -> log.info("SendResult: {}", res));
    }
}
