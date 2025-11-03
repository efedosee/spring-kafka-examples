package ru.otus.springkafkaexamples.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
@Slf4j
public class AppProducer {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final static String topic = "test-topic-2";
    private final static Random random = new Random();

    //    @Scheduled(fixedRate = 10_000)
    public void sendMessagesWithKafkaTemplateTransaction() {
        String randomValue1 = String.valueOf(random.nextInt(1000));
        String randomValue2 = String.valueOf(random.nextInt(1000));

        kafkaTemplate.executeInTransaction(t -> {
            t.send(topic, randomValue1, "test-".concat(randomValue1))
                    .thenAccept(result -> log.info("Sent first message, result: {}", result));

            try {
                Thread.sleep(5_000);
            } catch (InterruptedException e) {
                return false;
            }

            t.send(topic, randomValue2, "test-".concat(randomValue2))
                    .thenAccept(result -> log.info("Sent second message, result: {}", result));
            return true;
        });
    }

    //    @Scheduled(fixedRate = 10_000)
    public void sendSecondMessagesNotInTransaction() {
        String randomValue1 = String.valueOf(random.nextInt(1000));
        String randomValue2 = String.valueOf(random.nextInt(1000));

        kafkaTemplate.executeInTransaction(t ->
                t.send(topic, randomValue1, "test-".concat(randomValue1))
                        .thenAccept(result -> log.info("Sent first message, result: {}", result))
                        .thenComposeAsync(
                                v -> t.send(topic, randomValue2, "test-".concat(randomValue2)),
                                CompletableFuture.delayedExecutor(5, TimeUnit.SECONDS)
                        )
                        .thenAccept(result ->
                                log.info("Sent second message, result: {}", result))
                        .thenApply(r -> true)
                        .exceptionally(ex -> {
                            log.error("Exception while transaction execution:", ex);
                            return false;
                        })
                        .join());
    }

    @Scheduled(fixedRate = 10_000)
    @Transactional
    public void sendMessagesWithTransactionManager() {
        String randomValue1 = String.valueOf(random.nextInt(1000));
        String randomValue2 = String.valueOf(random.nextInt(1000));

        kafkaTemplate.send(topic, randomValue1, "test-".concat(randomValue1))
                .thenAccept(result -> log.info("Sent first message, result: {}", result));

        try {
            Thread.sleep(5_000);
        } catch (InterruptedException e) {
            log.error("InterruptedException", e);
        }

        kafkaTemplate.send(topic, randomValue2, "test-".concat(randomValue2))
                .thenAccept(result -> log.info("Sent second message, result: {}", result));
    }
}
