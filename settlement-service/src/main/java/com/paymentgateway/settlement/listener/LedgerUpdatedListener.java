package com.paymentgateway.settlement.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentgateway.events.KafkaTopics;
import com.paymentgateway.events.LedgerUpdatedEvent;
import com.paymentgateway.settlement.service.LedgerEventIngestService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class LedgerUpdatedListener {

    private final ObjectMapper objectMapper;
    private final LedgerEventIngestService ledgerEventIngestService;

    @KafkaListener(
            topics = KafkaTopics.PAYMENT_LEDGER_UPDATED,
            groupId = "settlement-service",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onLedgerUpdated(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            LedgerUpdatedEvent event = objectMapper.readValue(record.value(), LedgerUpdatedEvent.class);
            ledgerEventIngestService.recordIfNew(event);
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Failed to process payment.ledger.updated record at offset={} partition={}: {}",
                    record.offset(), record.partition(), e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
