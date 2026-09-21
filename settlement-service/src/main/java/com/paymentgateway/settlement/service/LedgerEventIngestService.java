package com.paymentgateway.settlement.service;

import com.paymentgateway.events.LedgerUpdatedEvent;
import com.paymentgateway.settlement.domain.ProcessedEvent;
import com.paymentgateway.settlement.domain.SettlementItem;
import com.paymentgateway.settlement.repository.ProcessedEventRepository;
import com.paymentgateway.settlement.repository.SettlementItemRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Slf4j
@Service
@RequiredArgsConstructor
public class LedgerEventIngestService {

    private final SettlementItemRepository settlementItemRepository;
    private final ProcessedEventRepository processedEventRepository;

    // Pre-check (existsById) rather than try/insert/catch: Hibernate defers
    // a plain save()'s INSERT until flush, so a unique-constraint violation
    // would not reliably surface at the save() call site -- see the longer
    // note in ledger-service's LedgerPostingService.postIfNew(). The unique
    // constraint remains as a backstop for genuine concurrent races.
    @Transactional
    public void recordIfNew(LedgerUpdatedEvent event) {
        if (processedEventRepository.existsById(event.getEventId())) {
            log.info("eventId={} already queued for settlement (redelivery) -- skipping", event.getEventId());
            return;
        }

        processedEventRepository.save(ProcessedEvent.builder()
                .eventId(event.getEventId())
                .processedAt(Instant.now())
                .build());

        settlementItemRepository.save(SettlementItem.builder()
                .paymentId(event.getPaymentId())
                .merchantId(event.getMerchantId())
                .amount(event.getAmount())
                .currency(event.getCurrency())
                .settled(false)
                .createdAt(Instant.now())
                .build());

        log.info("Queued paymentId={} for merchantId={} settlement", event.getPaymentId(), event.getMerchantId());
    }
}
