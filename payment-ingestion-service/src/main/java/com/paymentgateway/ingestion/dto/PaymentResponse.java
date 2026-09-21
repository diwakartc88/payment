package com.paymentgateway.ingestion.dto;

import com.paymentgateway.ingestion.domain.PaymentStatus;
import lombok.Builder;

import java.math.BigDecimal;
import java.time.Instant;

@Builder
public record PaymentResponse(
        String paymentId,
        String accountId,
        String merchantId,
        BigDecimal amount,
        String currency,
        PaymentStatus status,
        Instant createdAt,
        boolean replayed // true when this response is for a retried request with the same Idempotency-Key
) {
}
