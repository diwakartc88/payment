package com.paymentgateway.ingestion.outbox;

import com.paymentgateway.ingestion.domain.OutboxEvent;
import com.paymentgateway.ingestion.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Limit;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Polls the outbox table and publishes unpublished rows to Kafka.
 *
 * This is intentionally at-least-once, not exactly-once: if the process
 * crashes between a successful Kafka send and marking the row published,
 * the row is republished on restart. Every downstream consumer is required
 * to be idempotent on eventId (ARCHITECTURE.md §5.2), which is what makes
 * that safe rather than a bug.
 *
 * PRODUCTION NOTE: a single poller instance scanning one table does not
 * scale to 166K+ TPS on its own. SCALING.md covers the two real options:
 * (a) Debezium/CDC tailing the Postgres WAL instead of polling, or
 * (b) sharding the outbox table itself alongside the Payment table and
 * running one poller per shard. The Kafka publish code below is unchanged
 * either way -- only how rows are discovered changes.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxPublisher {

    private static final int BATCH_SIZE = 500;

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Scheduled(fixedDelayString = "${outbox.poll-interval-ms:200}")
    public void publishPending() {
        List<OutboxEvent> pending = outboxEventRepository.findByPublishedAtIsNullOrderByCreatedAtAsc(Limit.of(BATCH_SIZE));
        if (pending.isEmpty()) {
            return;
        }
        log.debug("Publishing {} pending outbox event(s)", pending.size());
        for (OutboxEvent event : pending) {
            publishOne(event);
        }
    }

    private void publishOne(OutboxEvent event) {
        CompletableFuture<?> future = kafkaTemplate.send(event.getTopic(), event.getPartitionKey(), event.getPayload());
        future.whenComplete((result, ex) -> {
            if (ex != null) {
                // Left unpublished -- the next poll tick retries it. A
                // production deployment alerts on outbox age (now - createdAt
                // for the oldest unpublished row) so a stuck Kafka connection
                // pages someone instead of silently backing up.
                log.error("Failed to publish outbox event id={} topic={} key={}: {}",
                        event.getId(), event.getTopic(), event.getPartitionKey(), ex.getMessage());
                return;
            }
            markPublished(event.getId());
        });
    }

    @Transactional
    protected void markPublished(Long outboxEventId) {
        outboxEventRepository.findById(outboxEventId).ifPresent(row -> {
            row.setPublishedAt(Instant.now());
            outboxEventRepository.save(row);
        });
    }
}
