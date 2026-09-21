package com.paymentgateway.events;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.math.BigDecimal;

/**
 * Published by fraud-detection-service after evaluating a PaymentInitiatedEvent.
 * Key: accountId.
 */
@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
public class FraudCheckedEvent extends BaseEvent {
    private String paymentId;
    private String accountId;
    private String merchantId;
    private BigDecimal amount;
    private String currency;
    private FraudDecision decision;
    private String reason;
}
