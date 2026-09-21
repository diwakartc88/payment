package com.paymentgateway.notification.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentgateway.events.KafkaTopics;
import com.paymentgateway.events.PaymentAuthorizedEvent;
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
public class PaymentAuthorizedListener {

    private final ObjectMapper objectMapper;
    private final IdempotencyGuard idempotencyGuard;
    private final WebhookDispatcher webhookDispatcher;

    @KafkaListener(
            topics = KafkaTopics.PAYMENT_AUTHORIZED,
            groupId = "notification-service",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onPaymentAuthorized(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            PaymentAuthorizedEvent event = objectMapper.readValue(record.value(), PaymentAuthorizedEvent.class);

            if (idempotencyGuard.markSentIfNew(event.getEventId())) {
                webhookDispatcher.dispatch("payment." + event.getStatus().name().toLowerCase(),
                        event.getMerchantId(), event.getPaymentId(),
                        event.getAmount() + " " + event.getCurrency());
            }
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Failed to process payment.authorized record at offset={} partition={}: {}",
                    record.offset(), record.partition(), e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
