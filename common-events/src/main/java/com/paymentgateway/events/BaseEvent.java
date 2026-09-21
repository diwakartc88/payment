package com.paymentgateway.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.time.Instant;
import java.util.UUID;

/**
 * Common envelope fields carried by every event on the bus.
 *
 * traceId is propagated end-to-end (HTTP -> Kafka -> HTTP) so a single payment
 * can be followed through all seven services in a distributed tracing tool.
 *
 * eventId is the idempotency key every consumer dedupes on (see
 * ARCHITECTURE.md §5.2) -- it must be unique per logical event, and must be
 * preserved (not regenerated) on redelivery/retry.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class BaseEvent {

    @Builder.Default
    private String eventId = UUID.randomUUID().toString();

    private String eventType;

    private String traceId;

    @Builder.Default
    private Instant occurredAt = Instant.now();

    @Builder.Default
    private int schemaVersion = 1;
}
