package com.paymentgateway.ledger.repository;

import com.paymentgateway.ledger.domain.LedgerEntry;
import org.springframework.data.jpa.repository.JpaRepository;

public interface LedgerEntryRepository extends JpaRepository<LedgerEntry, Long> {
}
