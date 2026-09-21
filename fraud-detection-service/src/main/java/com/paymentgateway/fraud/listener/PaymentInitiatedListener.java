package com.paymentgateway.fraud.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentgateway.events.FraudCheckedEvent;
import com.paymentgateway.events.KafkaTopics;
import com.paymentgateway.events.PaymentInitiatedEvent;
import com.paymentgateway.fraud.service.FraudRuleEngine;
import com.paymentgateway.fraud.service.IdempotencyGuard;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentInitiatedListener {

    private final ObjectMapper objectMapper;
    private final FraudRuleEngine fraudRuleEngine;
    private final IdempotencyGuard idempotencyGuard;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @KafkaListener(
            topics = KafkaTopics.PAYMENT_INITIATED,
            groupId = "fraud-detection-service",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onPaymentInitiated(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            PaymentInitiatedEvent event = objectMapper.readValue(record.value(), PaymentInitiatedEvent.class);

            if (!idempotencyGuard.markProcessedIfNew(event.getEventId())) {
                log.info("Skipping already-processed eventId={} (redelivery)", event.getEventId());
                ack.acknowledge();
                return;
            }

            FraudRuleEngine.Decision decision = fraudRuleEngine.evaluate(event);

            FraudCheckedEvent outEvent = FraudCheckedEvent.builder()
                    .eventType("PaymentFraudChecked")
                    .traceId(event.getTraceId())
                    .paymentId(event.getPaymentId())
                    .accountId(event.getAccountId())
                    .merchantId(event.getMerchantId())
                    .amount(event.getAmount())
                    .currency(event.getCurrency())
                    .decision(decision.decision())
                    .reason(decision.reason())
                    .build();

            String payload = objectMapper.writeValueAsString(outEvent);

            // Same partition key (accountId) as the inbound event -- keeps
            // this customer's events ordered on the next hop too.
            kafkaTemplate.send(KafkaTopics.PAYMENT_FRAUD_CHECKED, event.getAccountId(), payload)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("Failed to publish FraudCheckedEvent for paymentId={}: {}",
                                    event.getPaymentId(), ex.getMessage());
                            // Do NOT ack -- offset is not committed, message is redelivered.
                            // The idempotency guard above makes that safe.
                            return;
                        }
                        ack.acknowledge();
                    });

            log.info("paymentId={} accountId={} fraud decision={} reason={}",
                    event.getPaymentId(), event.getAccountId(), decision.decision(), decision.reason());

        } catch (Exception e) {
            log.error("Failed to process payment.initiated record at offset={} partition={}: {}",
                    record.offset(), record.partition(), e.getMessage(), e);
            // Left un-acked -- Spring Kafka's error handler (see application.yml
            // listener config) retries with backoff, then routes to the DLQ
            // topic after exhausting retries.
            throw new RuntimeException(e);
        }
    }
}
