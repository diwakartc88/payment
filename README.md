# payment-gateway

Event-driven payment gateway reference implementation. Java 21, Spring Boot 3.3,
Spring Cloud Gateway, Spring Kafka, PostgreSQL, Redis.

Read **[ARCHITECTURE.md](ARCHITECTURE.md)** first — it explains the design,
the patterns each service demonstrates (transactional outbox, idempotent
consumers, choreographed saga, double-entry ledger), and what's deliberately
out of scope. Read **[SCALING.md](SCALING.md)** for how this same
architecture is deployed to actually sustain 10M transactions/minute in
production.

## Services

| Service | Port | Role |
|---|---|---|
| api-gateway | 8080 | Edge routing, rate limiting, circuit breaking |
| payment-ingestion-service | 8081 | Accepts payments, transactional outbox |
| fraud-detection-service | 8082 | Kafka consumer/producer, velocity + rule checks |
| authorization-service | 8083 | Kafka consumer/producer, simulated issuer call |
| ledger-service | 8084 | Kafka consumer/producer, double-entry ledger |
| settlement-service | 8085 | Kafka consumer/producer, per-merchant settlement batches |
| notification-service | 8086 | Kafka consumer, webhook dispatch (logged, not a real HTTP call) |
| kafka-ui | 8090 | Browse topics/consumer groups (compose only) |

## Running locally

**Prerequisites:** Java 21, Maven 3.9+, Docker (with Compose v2).

### 1. Start infrastructure

```bash
docker compose up -d
```

This starts Kafka (KRaft mode, no Zookeeper), Postgres (with `ingestion_db`,
`ledger_db`, `settlement_db` created automatically), Redis, and Kafka UI
(http://localhost:8090). Topics are created automatically by the `kafka-init`
one-shot container — check it succeeded with `docker compose logs kafka-init`.

### 2. Build

```bash
mvn clean install
```

### 3. Run each service

Each is a normal Spring Boot app; run each in its own terminal:

```bash
mvn -pl api-gateway spring-boot:run
mvn -pl payment-ingestion-service spring-boot:run
mvn -pl fraud-detection-service spring-boot:run
mvn -pl authorization-service spring-boot:run
mvn -pl ledger-service spring-boot:run
mvn -pl settlement-service spring-boot:run
mvn -pl notification-service spring-boot:run
```

(Or containerize everything with `docker compose --profile apps up --build`
instead of steps 2-3.)

### 4. Send a payment through the pipeline

```bash
curl -i -X POST http://localhost:8080/api/v1/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{
    "accountId": "acct-1001",
    "merchantId": "merch-77",
    "amount": 49.99,
    "currency": "USD"
  }'
```

Expect `202 Accepted` with a `paymentId`. Watch it flow through the pipeline
in each service's logs (all tagged with the same `traceId`):

```
payment-ingestion-service : Ingested paymentId=... -> outbox row queued
fraud-detection-service   : fraud decision=APPROVED
authorization-service     : authorization status=AUTHORIZED
ledger-service             : Posted ledger entries: debit#.. credit#..
notification-service      : [WEBHOOK] -> merchantId=merch-77 eventType=payment.authorized
settlement-service         : (next batch cycle, default 60s) Settled batchId=...
notification-service      : [WEBHOOK] -> eventType=settlement.completed
```

**Retry the exact same request** (same `Idempotency-Key`) and you'll get
`200 OK` with `"replayed": true` and the *original* `paymentId` — no
duplicate payment, no duplicate ledger entries, no duplicate webhook (see
ARCHITECTURE.md §5.2).

**Trigger a decline** by sending an amount ending in `.13` (e.g. `10.13`) —
the simulated issuer in `IssuerSimulatorClient` deterministically declines
those, so you can see the decline path without needing real fraud/velocity
conditions.

**Trigger a fraud decline** by sending more than 20 requests for the same
`accountId` within a minute, or an amount over 10000.

### 5. Inspect topics

Open http://localhost:8090 (Kafka UI) to watch messages flow through
`payment.initiated` → `payment.fraud.checked` → `payment.authorized` →
`payment.ledger.updated` → `payment.settled`, and to check consumer lag per
group.

## What to read in the code, in order

1. `common-events/.../events/*.java` — the event schemas every service shares.
2. `payment-ingestion-service/.../service/PaymentIngestionService.java` +
   `.../outbox/OutboxPublisher.java` — the transactional outbox pattern.
3. `fraud-detection-service/.../listener/PaymentInitiatedListener.java` +
   `.../service/IdempotencyGuard.java` — the idempotent-consumer pattern.
4. `ledger-service/.../service/LedgerPostingService.java` — dedup +
   double-entry posting + outbox enqueue in one transaction (the
   "exactly-once effect" pattern from ARCHITECTURE.md §5.3).
5. `authorization-service/.../service/IssuerSimulatorClient.java` — circuit
   breaker + retry around a simulated unreliable downstream call.

## Known limitations of this reference implementation

See ARCHITECTURE.md §8 for the full list (PCI scope, real issuer
integration, CQRS read side, Avro/Schema Registry, multi-region). This is a
correctness-first skeleton meant to run on a laptop, not a certified payment
processor.
