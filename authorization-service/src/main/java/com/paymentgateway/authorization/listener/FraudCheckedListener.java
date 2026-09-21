package com.paymentgateway.authorization.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.paymentgateway.authorization.service.IdempotencyGuard;
import com.paymentgateway.authorization.service.IssuerSimulatorClient;
import com.paymentgateway.events.FraudCheckedEvent;
import com.paymentgateway.events.FraudDecision;
import com.paymentgateway.events.KafkaTopics;
import com.paymentgateway.events.PaymentAuthorizedEvent;
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
public class FraudCheckedListener {

    private final ObjectMapper objectMapper;
    private final IssuerSimulatorClient issuerClient;
    private final IdempotencyGuard idempotencyGuard;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @KafkaListener(
            topics = KafkaTopics.PAYMENT_FRAUD_CHECKED,
            groupId = "authorization-service",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onFraudChecked(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            FraudCheckedEvent event = objectMapper.readValue(record.value(), FraudCheckedEvent.class);

            if (event.getDecision() != FraudDecision.APPROVED) {
                // DECLINED and MANUAL_REVIEW are not this service's concern:
                // notification-service subscribes to payment.fraud.checked
                // directly for declines, and MANUAL_REVIEW would route to a
                // human review queue in a full implementation (out of scope
                // here -- see ARCHITECTURE.md §8).
                log.info("paymentId={} fraud decision={} -> not forwarding to issuer", event.getPaymentId(), event.getDecision());
                ack.acknowledge();
                return;
            }

            if (!idempotencyGuard.markProcessedIfNew(event.getEventId())) {
                log.info("Skipping already-processed eventId={} (redelivery)", event.getEventId());
                ack.acknowledge();
                return;
            }

            IssuerSimulatorClient.AuthResult result = issuerClient.authorize(
                    event.getPaymentId(), event.getAccountId(), event.getAmount());

            PaymentAuthorizedEvent outEvent = PaymentAuthorizedEvent.builder()
                    .eventType("PaymentAuthorized")
                    .traceId(event.getTraceId())
                    .paymentId(event.getPaymentId())
                    .accountId(event.getAccountId())
                    .merchantId(event.getMerchantId())
                    .amount(event.getAmount())
                    .currency(event.getCurrency())
                    .status(result.status())
                    .authorizationCode(result.authorizationCode())
                    .reason(result.reason())
                    .build();

            String payload = objectMapper.writeValueAsString(outEvent);

            kafkaTemplate.send(KafkaTopics.PAYMENT_AUTHORIZED, event.getAccountId(), payload)
                    .whenComplete((res, ex) -> {
                        if (ex != null) {
                            log.error("Failed to publish PaymentAuthorizedEvent for paymentId={}: {}",
                                    event.getPaymentId(), ex.getMessage());
                            return;
                        }
                        ack.acknowledge();
                    });

            log.info("paymentId={} accountId={} authorization status={}",
                    event.getPaymentId(), event.getAccountId(), result.status());

        } catch (Exception e) {
            log.error("Failed to process payment.fraud.checked record at offset={} partition={}: {}",
                    record.offset(), record.partition(), e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
