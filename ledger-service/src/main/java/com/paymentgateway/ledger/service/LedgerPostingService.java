package com.paymentgateway.ledger.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentgateway.events.AuthorizationStatus;
import com.paymentgateway.events.KafkaTopics;
import com.paymentgateway.events.LedgerUpdatedEvent;
import com.paymentgateway.events.PaymentAuthorizedEvent;
import com.paymentgateway.ledger.domain.EntryType;
import com.paymentgateway.ledger.domain.LedgerEntry;
import com.paymentgateway.ledger.domain.OutboxEvent;
import com.paymentgateway.ledger.domain.ProcessedEvent;
import com.paymentgateway.ledger.repository.LedgerEntryRepository;
import com.paymentgateway.ledger.repository.OutboxEventRepository;
import com.paymentgateway.ledger.repository.ProcessedEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

/**
 * The transactional core described in ARCHITECTURE.md §5.3: dedup +
 * double-entry posting + outbox enqueue, all in one local Postgres
 * transaction.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class LedgerPostingService {

    private final LedgerEntryRepository ledgerEntryRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    /**
     * @return true if this call actually posted new ledger entries; false if
     *         the event had already been processed (safe redelivery no-op).
     *
     * NOTE on the dedup check below: this is a SELECT-then-INSERT, not a
     * try/insert/catch. Hibernate defers the INSERT for a plain save() until
     * flush (normally at transaction commit), so a DataIntegrityViolation on
     * the processed_events unique constraint would surface at commit time --
     * AFTER the ledger entries and outbox row below had already been queued
     * in the same persistence context -- not at the save() call site. A
     * try/catch around save() alone would not reliably catch it before the
     * rest of this method runs. The unique constraint on event_id still
     * exists as a hard backstop against a genuine concurrent race; if it
     * ever fires, the whole transaction rolls back and Kafka redelivers,
     * at which point this pre-check catches it cleanly on the next attempt.
     */
    @Transactional
    public boolean postIfNew(PaymentAuthorizedEvent event) {
        if (event.getStatus() != AuthorizationStatus.AUTHORIZED) {
            // Declines never touch the ledger -- no money moved.
            return false;
        }

        if (processedEventRepository.existsById(event.getEventId())) {
            log.info("eventId={} already posted to the ledger (redelivery) -- skipping", event.getEventId());
            return false;
        }

        processedEventRepository.save(ProcessedEvent.builder()
                .eventId(event.getEventId())
                .processedAt(Instant.now())
                .build());

        Instant now = Instant.now();

        LedgerEntry debit = ledgerEntryRepository.save(LedgerEntry.builder()
                .paymentId(event.getPaymentId())
                .ledgerAccountId("ACCOUNT:" + event.getAccountId())
                .entryType(EntryType.DEBIT)
                .amount(event.getAmount())
                .currency(event.getCurrency())
                .createdAt(now)
                .build());

        LedgerEntry credit = ledgerEntryRepository.save(LedgerEntry.builder()
                .paymentId(event.getPaymentId())
                .ledgerAccountId("MERCHANT:" + event.getMerchantId())
                .entryType(EntryType.CREDIT)
                .amount(event.getAmount())
                .currency(event.getCurrency())
                .createdAt(now)
                .build());

        LedgerUpdatedEvent outEvent = LedgerUpdatedEvent.builder()
                .eventType("LedgerUpdated")
                .traceId(event.getTraceId())
                .paymentId(event.getPaymentId())
                .accountId(event.getAccountId())
                .merchantId(event.getMerchantId())
                .amount(event.getAmount())
                .currency(event.getCurrency())
                .debitEntryId(debit.getId())
                .creditEntryId(credit.getId())
                .build();

        outboxEventRepository.save(OutboxEvent.builder()
                .aggregateId(event.getPaymentId())
                .partitionKey(event.getMerchantId()) // re-keyed for settlement -- ARCHITECTURE.md §4
                .eventType(outEvent.getEventType())
                .topic(KafkaTopics.PAYMENT_LEDGER_UPDATED)
                .payload(serialize(outEvent))
                .traceId(event.getTraceId())
                .createdAt(now)
                .build());

        log.info("Posted ledger entries for paymentId={}: debit#{} credit#{} amount={} {}",
                event.getPaymentId(), debit.getId(), credit.getId(), event.getAmount(), event.getCurrency());

        return true;
    }

    private String serialize(LedgerUpdatedEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize LedgerUpdatedEvent for outbox", e);
        }
    }
}
