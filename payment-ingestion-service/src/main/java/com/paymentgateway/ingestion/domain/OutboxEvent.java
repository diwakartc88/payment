package com.paymentgateway.ingestion.domain;

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

/**
 * Transactional outbox row (ARCHITECTURE.md §5.1). Written in the SAME local
 * transaction as the Payment row it describes, so "payment accepted" and
 * "event will be published" can never disagree.
 *
 * At production scale this table is tailed by Debezium (CDC off the WAL)
 * instead of the polling OutboxPublisher in this reference impl -- polling
 * is simpler to read and reason about, CDC removes the polling latency and
 * DB read load at 166K+ TPS. Swapping the two doesn't change this schema.
 */
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
    private String aggregateId; // paymentId -- also used as the Kafka message key's source

    @Column(name = "partition_key", nullable = false)
    private String partitionKey; // accountId

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(name = "topic", nullable = false)
    private String topic;

    // Plain TEXT, not @Lob: Hibernate 6 maps @Lob String fields to
    // PostgreSQL's `oid` large-object type by default, not `text`, which
    // breaks ordinary string reads/writes. An explicit columnDefinition
    // avoids Hibernate's default 255-char varchar for a String column too.
    @Column(name = "payload", nullable = false, columnDefinition = "TEXT")
    private String payload; // JSON-serialized domain event

    @Column(name = "trace_id")
    private String traceId;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt; // null until the poller confirms the Kafka send
}
