# Scaling to 10,000,000 transactions/minute in production

This document is the bridge between the reference implementation in this
repo (correct, runnable on a laptop, a few hundred TPS) and a deployment
that actually sustains **166,667 TPS** (10M/min) with headroom for 2-3x
bursts (~350,000-500,000 TPS peak). Nothing here requires rewriting the
application code in this repo — every number below is a deployment
parameter (partition count, replica count, instance type, shard count).

Every number in this document is a **worked example with stated
assumptions**, not a guarantee. The one non-negotiable step before trusting
any of it in production is **load testing your actual message sizes and
hardware** (Phase 1 in ARCHITECTURE.md §9) and adjusting the constants
below accordingly.

---

## 1. Baseline numbers

- Target sustained throughput: **166,667 TPS** (10,000,000 / 60).
- Target peak (2-3x burst, typical for payment traffic around promotions/holidays): **350,000-500,000 TPS**. All sizing below is done against **500,000 TPS peak**.
- A "transaction" in this pipeline is actually **5 Kafka messages** (initiated → fraud-checked → authorized → ledger-updated → settled, ignoring DLQ/retry traffic), so the Kafka cluster's *aggregate* message rate at peak is closer to **2,500,000 msgs/sec** across all topics combined — but no single consumer or partition ever needs to process more than one topic's share of that.
- Assumed average message size: **300 bytes** (Avro-encoded, see §3) before compression, **~150 bytes** after `lz4`. The reference implementation's JSON encoding runs **2-3x larger** — this is the single biggest reason JSON is fine for a laptop demo and not for this scale (see §3).

---

## 2. Kafka cluster sizing

### 2.1 Partitions per topic

Partition count is bounded below by *"can each partition sustain its share of peak throughput,"* and bounded above by *"how many partitions can this cluster's controller and this topic's consumers reasonably manage."*

**Per-partition sustainable throughput** depends on message size, `acks` setting, replication factor, and broker hardware. A commonly-achievable, conservative planning number for `acks=all`, `replication.factor=3`, small (~150-300 byte) messages on NVMe-backed brokers is **8,000-10,000 messages/sec/partition** sustained. (Kafka can go well above this on ideal hardware with larger messages and heavy batching — this number is deliberately conservative for capacity planning, not a hard ceiling. Validate it with your own load test.)

For a hot topic at 500,000 msgs/sec peak:

```
partitions_needed = peak_msgs_per_sec / safe_msgs_per_sec_per_partition
                   = 500,000 / 8,000
                   ≈ 63
```

Round up generously for (a) partition-key skew — some accounts/merchants are hotter than others, so the *busiest* partition needs headroom, not just the average — and (b) growth without a disruptive repartition later. **256 partitions** per hot topic (`payment.initiated`, `payment.fraud.checked`, `payment.authorized`) is a reasonable production target: ~4x the computed minimum, a round number for tooling, and in line with partition counts real large-scale event pipelines run.

`payment.ledger.updated` and `payment.settled` carry materially less traffic (settlement batches, not raw transactions) — **64 partitions** is generous headroom there.

DLQ topics see a tiny fraction of traffic (retries that exhausted backoff) — **12 partitions** each is plenty.

### 2.2 Broker count

Total cluster write throughput (bytes/sec) at peak, across the three hot topics, compressed:

```
3 topics × 500,000 msgs/sec × 150 bytes ≈ 225 MB/s raw producer writes
```

Replication factor 3 means every write is also shipped to 2 follower replicas — the *cluster-internal* write bandwidth is roughly 3x the producer-facing figure, i.e. **~675 MB/s** of replication traffic, plus consumer fetch traffic (every message is read by at least one consumer group per topic, several topics have 2, e.g. `payment.authorized` is read by both `ledger-service` and `notification-service`) which — served largely from the OS page cache rather than disk — is a network, not disk, bound.

A conservative per-broker sustained throughput budget (leaving headroom for compaction, controller traffic, and not running brokers at the redline) on modern NVMe-backed, 10-25 Gbps-networked instances is **~150-250 MB/s**. That puts a production cluster at:

```
brokers_needed ≈ 675 MB/s / 200 MB/s per broker ≈ 34, round up for AZ-balance and N+2 fault tolerance → 40-60 brokers
```

Spread across **3+ availability zones**, rack-aware (`broker.rack`), so a single AZ failure doesn't take partitions below their minimum ISR.

### 2.3 Producer/consumer configuration at scale

Producer (every service that publishes):
```
acks=all
enable.idempotence=true
compression.type=lz4          # or zstd for better ratio at a small CPU cost
linger.ms=5-20                # batch aggressively; small linger costs little latency at this volume
batch.size=131072              # 128KB, larger than the default 16KB -- fewer, fatter requests
max.in.flight.requests.per.connection=5   # safe with idempotence on
```

Consumer (every service that subscribes):
```
isolation.level=read_committed
fetch.min.bytes=50000          # wait for a meaningful batch rather than fetching on every message
fetch.max.wait.ms=100
max.poll.records=500-1000      # tune down for CPU-bound stages (fraud rules), up for I/O-bound (notification)
```

Consumer **concurrency must scale with partition count**: with 256 partitions on `payment.initiated`, `fraud-detection-service` needs up to 256 consumer instances (or fewer instances each running multiple threads via `ConcurrentKafkaListenerContainerFactory.setConcurrency()`, capped at partitions-per-instance) to have every partition actively consumed. This is the concrete mechanism behind ARCHITECTURE.md §2's claim that fraud-detection scales independently of the other stages — it's the stage most likely to need to run closer to the full 256-way parallelism because rule evaluation is the most CPU-bound hop in the pipeline.

---

## 3. Avro + Schema Registry (replacing JSON)

The reference implementation serializes events as JSON strings for readability. At 500,000 msgs/sec, JSON's overhead compounds:
- **2-3x larger payloads** than an equivalent Avro binary encoding → proportionally more network and disk I/O for the same transaction volume.
- **Slower serialize/deserialize** (text parsing vs. binary schema-driven decoding) → more CPU per message, which is the resource most likely to bottleneck a high-throughput consumer.
- **No schema enforcement** — a producer bug that emits a malformed field is only caught at the consumer, often after it's already in the topic.

Production setup: **Confluent Schema Registry** (or an equivalent, e.g. AWS Glue Schema Registry / Karapace), Avro schemas generated from the same domain model as `common-events`, `KafkaAvroSerializer`/`KafkaAvroDeserializer` in place of the `StringSerializer`/`StringDeserializer` used in this repo. Schema evolution rules (`BACKWARD` compatibility at minimum) let you add fields to events without a synchronized deploy of every consuming service — critical when you have 7+ independently-deployed consumers of the same topic family.

---

## 4. Database sharding

A single Postgres instance tops out (generously, with excellent hardware and tuning) around **tens of thousands of writes/sec** — nowhere near 166,667 TPS sustained. `payment-ingestion-service` and `ledger-service` both write once per transaction, so both need to scale past a single instance.

**Sharding key: `accountId` hash** (ingestion) and a scheme that keeps both ledger sides of a transaction — the debit (`accountId`) and credit (`merchantId`) rows — reachable without a cross-shard transaction. Two common approaches, in increasing order of operational complexity:

1. **Shard Postgres itself** by `hash(accountId) % N`, using something like Citus (Postgres extension) or manual application-level routing to N independent Postgres clusters. Simpler to reason about (still SQL, still ACID *within* a shard), but cross-shard queries (e.g. "this merchant's total across all their customers' shards") need a separate read-side aggregation — which you want anyway (§6).
2. **Move to a natively-distributed database** for the ledger specifically — CockroachDB, YugabyteDB, or Spanner-like systems give you distributed ACID transactions without hand-rolled shard routing, at the cost of higher write latency per transaction (multi-node consensus) and a bigger operational learning curve than Postgres.

Either way, **the outbox pattern (§5.1/§5.3 in ARCHITECTURE.md) is what makes this tractable**: the outbox table shards along with its owning table (same shard, same local transaction), so sharding the business data and sharding the outbox are the same operation, not two.

The **outbox poller itself must also shard** — one poller process per shard (or a CDC connector, e.g. Debezium, per shard) rather than one process scanning a single now-nonexistent global table.

---

## 5. Consumer autoscaling

Fixed replica counts waste money off-peak and fall behind during bursts. Autoscale each consuming service on **Kafka consumer lag**, not CPU — CPU can look idle while a service is actually falling behind on I/O-bound work (or vice versa: CPU can spike on GC without any real backlog).

**KEDA** (Kubernetes Event-Driven Autoscaling) with its Kafka scaler is the standard tool: it queries consumer group lag per topic directly and scales the deployment's replica count against a target lag threshold (e.g. "keep lag under 10,000 messages per partition, scale up if exceeded, scale down after a cooldown once lag clears"). Cap `maxReplicaCount` at the topic's partition count — beyond that, extra replicas sit idle with no partitions assigned.

---

## 6. Read-side (CQRS) — not fully built in this repo

Nothing in this pipeline is optimized for "what's the status of payment X" or "what's merchant Y's transaction volume today" — those are query patterns, and the write-path databases here are sharded and optimized for writes, not ad-hoc queries. Production adds a **read-side projector**: a consumer (or several, per query shape) subscribed to the same topics, writing into a query-optimized store (Elasticsearch/OpenSearch for search, a wide read replica or a materialized-view database for dashboards). This is standard CQRS and is exactly why the write path never needs to serve reads directly — it can stay narrowly optimized for write throughput.

---

## 7. Idempotency & rate-limiting infrastructure

The reference implementation's single Redis instance for idempotency guards and velocity counters becomes a **Redis Cluster** (sharded, with replicas) at this scale — both for throughput (a single Redis instance also tops out well below 500K ops/sec under realistic latency budgets) and for availability (a single instance is a single point of failure for every service's dedup check). Idempotency keys and velocity counters are both naturally shardable by `accountId`/`eventId`, so cluster mode's hash-slot sharding fits without any application-level routing logic.

---

## 8. Multi-region & disaster recovery

Out of scope for the reference implementation (ARCHITECTURE.md §8) but the shape of it: **active-active by region**, with Kafka clusters mirrored region-to-region (e.g. via Kafka's MirrorMaker 2 or a managed equivalent) for DR read access, while each region's traffic is normally served by that region's own cluster to avoid cross-region latency on the hot path. Payment routing typically pins a given `accountId` to a home region (consistent hashing) so a single customer's transactions don't need cross-region coordination in the common case, with a documented (and tested) failover runbook for the case where a whole region is unavailable.

---

## 9. Load testing methodology

Before trusting any number above: build a producer load generator (`kafka-producer-perf-test.sh` for raw Kafka throughput, or a purpose-built load test hitting `api-gateway` end-to-end for realistic pipeline testing) and answer, for **your actual message sizes and hardware**:

1. What's the real sustained msgs/sec/partition before p99 latency or error rate degrades?
2. Where does the pipeline's end-to-end latency (ingestion → settled) sit at target load, and which stage dominates it?
3. What's the actual failure mode under 2x sustained overload — does back pressure propagate cleanly (growing consumer lag, visible in monitoring) or does something fall over (OOM, connection pool exhaustion, cascading timeouts)?

Re-run this after every material change to partition counts, instance types, or consumer concurrency — the numbers in this document are a starting point for that testing, not a substitute for it.
