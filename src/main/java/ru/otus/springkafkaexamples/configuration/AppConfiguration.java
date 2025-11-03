package ru.otus.springkafkaexamples.configuration;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.kafka.config.TopicBuilder;

import javax.sql.DataSource;

@Configuration
public class AppConfiguration {
    @Bean
    public NewTopic testTopic1() {
        return TopicBuilder.name("test-topic-3")
                .partitions(2)
                .build();
    }

    @Bean
    public DataSourceTransactionManager dataSourceTransactionManager(DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }
}
