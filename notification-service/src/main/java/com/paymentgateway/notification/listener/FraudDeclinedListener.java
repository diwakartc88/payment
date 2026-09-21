package com.paymentgateway.notification.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentgateway.events.FraudCheckedEvent;
import com.paymentgateway.events.FraudDecision;
import com.paymentgateway.events.KafkaTopics;
import com.paymentgateway.notification.service.IdempotencyGuard;
import com.paymentgateway.notification.service.WebhookDispatcher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Payments declined by fraud-detection-service never reach
 * authorization-service, so this is the only place that event surfaces --
 * hence notification-service subscribes to payment.fraud.checked directly
 * rather than only to payment.authorized (ARCHITECTURE.md, service table).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FraudDeclinedListener {

    private final ObjectMapper objectMapper;
    private final IdempotencyGuard idempotencyGuard;
    private final WebhookDispatcher webhookDispatcher;

    @KafkaListener(
            topics = KafkaTopics.PAYMENT_FRAUD_CHECKED,
            groupId = "notification-service",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onFraudChecked(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            FraudCheckedEvent event = objectMapper.readValue(record.value(), FraudCheckedEvent.class);

            if (event.getDecision() == FraudDecision.DECLINED && idempotencyGuard.markSentIfNew(event.getEventId())) {
                webhookDispatcher.dispatch("payment.declined", event.getMerchantId(), event.getPaymentId(), event.getReason());
            }
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Failed to process payment.fraud.checked record at offset={} partition={}: {}",
                    record.offset(), record.partition(), e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
