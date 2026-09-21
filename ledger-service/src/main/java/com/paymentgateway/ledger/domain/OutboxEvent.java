package com.paymentgateway.ledger.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/** Same shape/purpose as payment-ingestion-service's OutboxEvent -- see that class's Javadoc. */
@Entity
@Table(name = "outbox_events")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OutboxEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "aggregate_id", nullable = false)
    private String aggregateId; // paymentId

    @Column(name = "partition_key", nullable = false)
    private String partitionKey; // merchantId -- re-keyed, see ARCHITECTURE.md §4

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(name = "topic", nullable = false)
    private String topic;

    // Plain TEXT, not @Lob: Hibernate 6 maps @Lob String fields to
    // PostgreSQL's `oid` large-object type by default, not `text`, which
    // breaks ordinary string reads/writes. An explicit columnDefinition
    // avoids Hibernate's default 255-char varchar for a String column too.
    @Column(name = "payload", nullable = false, columnDefinition = "TEXT")
    private String payload;

    @Column(name = "trace_id")
    private String traceId;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;
}
