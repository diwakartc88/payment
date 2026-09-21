package com.paymentgateway.settlement.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * One ledgered payment awaiting settlement. Accumulated by
 * LedgerUpdatedListener as payment.ledger.updated events arrive, and
 * consumed in batches by SettlementBatchProcessor on a fixed cycle.
 *
 * Using a durable table (rather than an in-memory accumulator) means a
 * settlement-service restart never loses a payment that's waiting to be
 * settled -- it's just still sitting here with settled=false.
 */
@Entity
@Table(name = "settlement_items")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SettlementItem {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "payment_id", nullable = false)
    private String paymentId;

    @Column(name = "merchant_id", nullable = false)
    private String merchantId;

    @Column(nullable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(nullable = false, length = 3)
    private String currency;

    @Column(name = "settled", nullable = false)
    private boolean settled;

    @Column(name = "settlement_batch_id")
    private String settlementBatchId;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;
}
