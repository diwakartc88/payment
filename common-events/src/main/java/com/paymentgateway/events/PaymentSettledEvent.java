package com.paymentgateway.events;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.math.BigDecimal;
import java.util.List;

/**
 * Published by settlement-service when a batch of ledgered payments for a
 * merchant is settled (e.g. a payout cycle closes).
 * Key: merchantId.
 */
@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
public class PaymentSettledEvent extends BaseEvent {
    private String settlementBatchId;
    private String merchantId;
    private BigDecimal totalAmount;
    private String currency;
    private List<String> paymentIds;
}
