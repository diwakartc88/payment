package com.paymentgateway.events;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.math.BigDecimal;

/**
 * Published by ledger-service after atomically recording the double-entry
 * bookkeeping rows for an authorized payment.
 * Key: RE-KEYED to merchantId (settlement is a per-merchant operation --
 * see ARCHITECTURE.md §4 "Re-keying at the ledger boundary").
 */
@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
public class LedgerUpdatedEvent extends BaseEvent {
    private String paymentId;
    private String accountId;
    private String merchantId;
    private BigDecimal amount;
    private String currency;
    private Long debitEntryId;
    private Long creditEntryId;
}
