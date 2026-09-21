package com.paymentgateway.ledger.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
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
 * One row of a double-entry bookkeeping pair. Every authorized payment
 * produces exactly two rows: a DEBIT against the payer's account and a
 * CREDIT against the merchant's clearing account, for the same amount.
 * Rows are NEVER updated or deleted (append-only / event-sourced) --
 * corrections are new offsetting entries. This is both an audit
 * requirement and what makes concurrent writes lock-free (no UPDATE
 * contention on a running balance column).
 *
 * An account's current balance is a derived value: SUM(CREDIT) - SUM(DEBIT)
 * for that ledgerAccountId. A real system maintains a materialized balance
 * snapshot per account (updated by the same transaction, or by a downstream
 * projector) rather than re-summing the full history on every read -- left
 * as an extension point here.
 */
@Entity
@Table(name = "ledger_entries")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LedgerEntry {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "payment_id", nullable = false)
    private String paymentId;

    @Column(name = "ledger_account_id", nullable = false)
    private String ledgerAccountId; // e.g. "ACCOUNT:acc-123" or "MERCHANT:mer-456"

    @Enumerated(EnumType.STRING)
    @Column(name = "entry_type", nullable = false, length = 10)
    private EntryType entryType;

    @Column(nullable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(nullable = false, length = 3)
    private String currency;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;
}
