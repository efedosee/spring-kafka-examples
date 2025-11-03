package ru.otus.springkafkaexamples.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Random;

@Component
@RequiredArgsConstructor
@Slf4j
public class AppProducer {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final JdbcTemplate jdbcTemplate;
    private final static String topic = "test-topic-3";
    private final static Random random = new Random();

    // https://docs.spring.io/spring-kafka/reference/tips.html#ex-jdbc-sync
    @Scheduled(fixedRate = 10_000)
    @Transactional("dataSourceTransactionManager")
    public void sendMessagesWithMixedTransaction() {
        String randomValue = String.valueOf(random.nextInt(1000));

        this.kafkaTemplate.send(topic, randomValue, "test-".concat(randomValue))
                .exceptionally(ex -> {
                    log.error("Exception while sending message", ex);
                    return null;
                })
                .thenAccept(result -> log.info("Sent message, result: {}", result));

        this.jdbcTemplate.update("insert into my_table (data) values (?)", randomValue);
    }
}
