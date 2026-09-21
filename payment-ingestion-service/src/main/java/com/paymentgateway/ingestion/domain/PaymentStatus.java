package com.paymentgateway.ingestion.domain;

public enum PaymentStatus {
    RECEIVED,   // accepted by ingestion, outbox event not necessarily published yet
    PUBLISHED   // outbox event confirmed published to Kafka
    // Terminal states (AUTHORIZED / DECLINED / SETTLED) live downstream in
    // ledger-service / settlement-service -- ingestion does not own them.
    // A read-side projection (see ARCHITECTURE.md §6, CQRS) is what a
    // merchant's "GET /payments/{id}" status lookup would actually query.
}
