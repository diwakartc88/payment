package com.paymentgateway.ledger.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentgateway.events.KafkaTopics;
import com.paymentgateway.events.PaymentAuthorizedEvent;
import com.paymentgateway.ledger.service.LedgerPostingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentAuthorizedListener {

    private final ObjectMapper objectMapper;
    private final LedgerPostingService ledgerPostingService;

    @KafkaListener(
            topics = KafkaTopics.PAYMENT_AUTHORIZED,
            groupId = "ledger-service",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onPaymentAuthorized(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            PaymentAuthorizedEvent event = objectMapper.readValue(record.value(), PaymentAuthorizedEvent.class);

            // postIfNew() commits the DB transaction (ledger entries +
            // outbox row + dedup marker) before we return -- only THEN do we
            // acknowledge the Kafka offset. If the process crashes between
            // commit and ack, redelivery hits the processed_events unique
            // constraint and is a safe no-op (ARCHITECTURE.md §5.3).
            ledgerPostingService.postIfNew(event);
            ack.acknowledge();

        } catch (Exception e) {
            log.error("Failed to process payment.authorized record at offset={} partition={}: {}",
                    record.offset(), record.partition(), e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
