package com.paymentgateway.events;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.math.BigDecimal;

/**
 * Published by authorization-service after calling the issuer/card network
 * (simulated in this reference implementation).
 * Key: accountId.
 */
@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
public class PaymentAuthorizedEvent extends BaseEvent {
    private String paymentId;
    private String accountId;
    private String merchantId;
    private BigDecimal amount;
    private String currency;
    private AuthorizationStatus status;
    private String authorizationCode;
    private String reason;
}
