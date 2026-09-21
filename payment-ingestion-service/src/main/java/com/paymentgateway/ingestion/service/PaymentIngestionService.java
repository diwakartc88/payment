package com.paymentgateway.ingestion.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentgateway.events.KafkaTopics;
import com.paymentgateway.events.PaymentInitiatedEvent;
import com.paymentgateway.ingestion.domain.OutboxEvent;
import com.paymentgateway.ingestion.domain.Payment;
import com.paymentgateway.ingestion.domain.PaymentStatus;
import com.paymentgateway.ingestion.dto.PaymentRequest;
import com.paymentgateway.ingestion.dto.PaymentResponse;
import com.paymentgateway.ingestion.repository.OutboxEventRepository;
import com.paymentgateway.ingestion.repository.PaymentRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentIngestionService {

    private final PaymentRepository paymentRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    /**
     * Accepts a payment request idempotently and, in the SAME database
     * transaction, writes the outbox row that will become a
     * PaymentInitiatedEvent on Kafka (ARCHITECTURE.md §5.1).
     *
     * The unique constraint on (accountId, idempotencyKey) is the actual
     * safety net -- the pre-check below is an optimization to avoid a
     * needless exception-driven round trip on the common case of a genuine
     * retry, not the source of correctness.
     */
    @Transactional
    public PaymentResponse ingest(PaymentRequest request, String idempotencyKey, String traceId) {
        Optional<Payment> existing = paymentRepository.findByAccountIdAndIdempotencyKey(
                request.accountId(), idempotencyKey);
        if (existing.isPresent()) {
            log.info("Replayed request detected for accountId={} idempotencyKey={} -> returning original paymentId={}",
                    request.accountId(), idempotencyKey, existing.get().getId());
            return toResponse(existing.get(), true);
        }

        String paymentId = UUID.randomUUID().toString();
        Instant now = Instant.now();

        Payment payment = Payment.builder()
                .id(paymentId)
                .accountId(request.accountId())
                .merchantId(request.merchantId())
                .amount(request.amount())
                .currency(request.currency())
                .idempotencyKey(idempotencyKey)
                .status(PaymentStatus.RECEIVED)
                .createdAt(now)
                .build();
        paymentRepository.save(payment);

        PaymentInitiatedEvent event = PaymentInitiatedEvent.builder()
                .eventType("PaymentInitiated")
                .traceId(traceId)
                .paymentId(paymentId)
                .accountId(request.accountId())
                .merchantId(request.merchantId())
                .amount(request.amount())
                .currency(request.currency())
                .idempotencyKey(idempotencyKey)
                .build();

        OutboxEvent outboxEvent = OutboxEvent.builder()
                .aggregateId(paymentId)
                .partitionKey(request.accountId())
                .eventType(event.getEventType())
                .topic(KafkaTopics.PAYMENT_INITIATED)
                .payload(serialize(event))
                .traceId(traceId)
                .createdAt(now)
                .build();
        outboxEventRepository.save(outboxEvent);

        log.info("Ingested paymentId={} accountId={} amount={} {} -> outbox row queued for publish",
                paymentId, request.accountId(), request.amount(), request.currency());

        return toResponse(payment, false);
    }

    private String serialize(PaymentInitiatedEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (Exception e) {
            // A serialization failure here must fail the whole transaction --
            // we never want a Payment row committed without its outbox event.
            throw new IllegalStateException("Failed to serialize PaymentInitiatedEvent for outbox", e);
        }
    }

    private PaymentResponse toResponse(Payment payment, boolean replayed) {
        return PaymentResponse.builder()
                .paymentId(payment.getId())
                .accountId(payment.getAccountId())
                .merchantId(payment.getMerchantId())
                .amount(payment.getAmount())
                .currency(payment.getCurrency())
                .status(payment.getStatus())
                .createdAt(payment.getCreatedAt())
                .replayed(replayed)
                .build();
    }
}
