package com.paymentgateway.events;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.math.BigDecimal;

/**
 * Published by payment-ingestion-service (via the transactional outbox)
 * the moment a payment request has been durably accepted.
 * Key: accountId (see ARCHITECTURE.md §4 for why).
 */
@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
public class PaymentInitiatedEvent extends BaseEvent {
    private String paymentId;
    private String accountId;
    private String merchantId;
    private BigDecimal amount;
    private String currency;
    private String idempotencyKey;
}
