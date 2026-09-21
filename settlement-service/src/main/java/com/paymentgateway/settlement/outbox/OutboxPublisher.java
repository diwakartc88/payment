package com.paymentgateway.settlement.outbox;

import com.paymentgateway.settlement.domain.OutboxEvent;
import com.paymentgateway.settlement.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Limit;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

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
        for (OutboxEvent event : pending) {
            publishOne(event);
        }
    }

    private void publishOne(OutboxEvent event) {
        kafkaTemplate.send(event.getTopic(), event.getPartitionKey(), event.getPayload())
                .whenComplete((result, ex) -> {
                    if (ex != null) {
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
