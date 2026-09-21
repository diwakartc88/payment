package com.paymentgateway.settlement.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentgateway.events.KafkaTopics;
import com.paymentgateway.events.PaymentSettledEvent;
import com.paymentgateway.settlement.domain.OutboxEvent;
import com.paymentgateway.settlement.domain.SettlementItem;
import com.paymentgateway.settlement.repository.OutboxEventRepository;
import com.paymentgateway.settlement.repository.SettlementItemRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Drives a settlement cycle: every {@code settlement.batch-interval-ms}, every
 * merchant with unsettled items gets one settlement batch closed and one
 * PaymentSettledEvent enqueued to the outbox.
 *
 * A real payout schedule is typically daily/weekly per merchant contract
 * terms, not a fixed short interval -- this is set short here (60s default)
 * purely so the reference pipeline is observable end-to-end without a long
 * wait. Swap the cron/interval per environment.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SettlementBatchProcessor {

    private final SettlementItemRepository settlementItemRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    @Scheduled(fixedDelayString = "${settlement.batch-interval-ms:60000}")
    public void runSettlementCycle() {
        List<String> merchantIds = settlementItemRepository.findDistinctMerchantIdsWithUnsettledItems();
        if (merchantIds.isEmpty()) {
            return;
        }
        log.info("Running settlement cycle for {} merchant(s)", merchantIds.size());
        for (String merchantId : merchantIds) {
            settleMerchant(merchantId);
        }
    }

    @Transactional
    protected void settleMerchant(String merchantId) {
        List<SettlementItem> items = settlementItemRepository.findByMerchantIdAndSettledFalse(merchantId);
        if (items.isEmpty()) {
            return;
        }

        String batchId = UUID.randomUUID().toString();
        BigDecimal total = items.stream().map(SettlementItem::getAmount).reduce(BigDecimal.ZERO, BigDecimal::add);
        String currency = items.get(0).getCurrency(); // reference impl assumes single-currency merchants

        for (SettlementItem item : items) {
            item.setSettled(true);
            item.setSettlementBatchId(batchId);
        }
        settlementItemRepository.saveAll(items);

        PaymentSettledEvent event = PaymentSettledEvent.builder()
                .eventType("PaymentSettled")
                .settlementBatchId(batchId)
                .merchantId(merchantId)
                .totalAmount(total)
                .currency(currency)
                .paymentIds(items.stream().map(SettlementItem::getPaymentId).collect(Collectors.toList()))
                .build();

        outboxEventRepository.save(OutboxEvent.builder()
                .aggregateId(batchId)
                .partitionKey(merchantId)
                .eventType(event.getEventType())
                .topic(KafkaTopics.PAYMENT_SETTLED)
                .payload(serialize(event))
                .createdAt(Instant.now())
                .build());

        log.info("Settled batchId={} merchantId={} itemCount={} total={} {}",
                batchId, merchantId, items.size(), total, currency);
    }

    private String serialize(PaymentSettledEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize PaymentSettledEvent for outbox", e);
        }
    }
}
