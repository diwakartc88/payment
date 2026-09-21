package com.paymentgateway.ingestion.repository;

import com.paymentgateway.ingestion.domain.Payment;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface PaymentRepository extends JpaRepository<Payment, String> {
    Optional<Payment> findByAccountIdAndIdempotencyKey(String accountId, String idempotencyKey);
}
