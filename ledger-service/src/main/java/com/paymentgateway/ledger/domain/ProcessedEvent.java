package com.paymentgateway.ledger.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Consumer-side idempotency record (ARCHITECTURE.md §5.2 / §5.3). The
 * primary key IS the inbound eventId, so a redelivered payment.authorized
 * record fails this insert with a constraint violation inside the same
 * transaction as the ledger entries -- guaranteeing "ledger entries written"
 * and "event marked processed" rise or fall together.
 */
@Entity
@Table(name = "processed_events")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProcessedEvent {

    @Id
    @Column(name = "event_id", length = 36)
    private String eventId;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;
}
