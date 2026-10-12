package ru.otus.springkafkaexamples.producer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Random;

@Component
@Slf4j
public class AppProducer {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    private final static String topic = "test-topic-3";
    private final static Random random = new Random();

    public AppProducer(KafkaTemplate<String, String> kafkaTemplate, JdbcTemplate jdbcTemplate, DataSourceTransactionManager transactionManager) {
        this.kafkaTemplate = kafkaTemplate;
        this.jdbcTemplate = jdbcTemplate;

        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    // https://docs.spring.io/spring-kafka/reference/tips.html#ex-jdbc-sync
    @Scheduled(fixedRate = 10_000)
//    @Async("taskExecutor") // to execute this task in parallel
    public void sendMessagesWithMixedTransaction() {
        String randomValue = String.valueOf(random.nextInt(1000));
        transactionTemplate.executeWithoutResult((status) -> {
            log.debug("Adding {} into my_table", randomValue);
            this.jdbcTemplate.update("insert into my_table (data) values (?)", randomValue);

            this.kafkaTemplate.send(topic, randomValue, "test-".concat(randomValue))
                    .whenComplete((result, throwable) -> {
                        if (throwable != null) {
                            log.error("Exception while sending message: {}", throwable.getMessage());
                            status.setRollbackOnly();
                        } else {
                            log.info("Message have been sent, result: {}", result);
                        }
                    })
                    .join();
        });
    }
}
