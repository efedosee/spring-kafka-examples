package ru.otus.springkafkaexamples;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class SpringKafkaExamplesApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringKafkaExamplesApplication.class, args);
	}
}
