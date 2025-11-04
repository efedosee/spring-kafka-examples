package ru.otus.springkafkaexamples.dto.event;

import java.util.UUID;

public record Event(
        UUID id,
        EventType type,
        Payload payload
) {
}
