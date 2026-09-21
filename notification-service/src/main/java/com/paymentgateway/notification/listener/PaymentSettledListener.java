package com.paymentgateway.notification.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentgateway.events.KafkaTopics;
import com.paymentgateway.events.PaymentSettledEvent;
import com.paymentgateway.notification.service.IdempotencyGuard;
import com.paymentgateway.notification.service.WebhookDispatcher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentSettledListener {

    private final ObjectMapper objectMapper;
    private final IdempotencyGuard idempotencyGuard;
    private final WebhookDispatcher webhookDispatcher;

    @KafkaListener(
            topics = KafkaTopics.PAYMENT_SETTLED,
            groupId = "notification-service",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onPaymentSettled(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            PaymentSettledEvent event = objectMapper.readValue(record.value(), PaymentSettledEvent.class);

            if (idempotencyGuard.markSentIfNew(event.getEventId())) {
                webhookDispatcher.dispatch("settlement.completed", event.getMerchantId(), event.getSettlementBatchId(),
                        event.getTotalAmount() + " " + event.getCurrency() + " across " + event.getPaymentIds().size() + " payment(s)");
            }
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Failed to process payment.settled record at offset={} partition={}: {}",
                    record.offset(), record.partition(), e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
