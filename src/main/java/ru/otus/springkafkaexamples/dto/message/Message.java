package ru.otus.springkafkaexamples.dto.message;

import java.util.UUID;

public record Message(
        UUID id,
        String message
) {
}
