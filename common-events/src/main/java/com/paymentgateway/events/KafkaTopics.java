package com.paymentgateway.events;

/**
 * Central registry of topic names so no service hardcodes a string that can
 * drift out of sync with another service. Partition counts / replication
 * factors are NOT set here -- those are deployment-time (Terraform / Helm
 * values), not application code, since they change between local dev and
 * production (see SCALING.md).
 */
public final class KafkaTopics {

    private KafkaTopics() {
    }

    public static final String PAYMENT_INITIATED = "payment.initiated";
    public static final String PAYMENT_FRAUD_CHECKED = "payment.fraud.checked";
    public static final String PAYMENT_AUTHORIZED = "payment.authorized";
    public static final String PAYMENT_LEDGER_UPDATED = "payment.ledger.updated";
    public static final String PAYMENT_SETTLED = "payment.settled";

    // One dead-letter topic per consuming stage.
    public static final String DLQ_FRAUD = "payment.dlq.fraud";
    public static final String DLQ_AUTHORIZATION = "payment.dlq.authorization";
    public static final String DLQ_LEDGER = "payment.dlq.ledger";
    public static final String DLQ_SETTLEMENT = "payment.dlq.settlement";
    public static final String DLQ_NOTIFICATION = "payment.dlq.notification";
}
