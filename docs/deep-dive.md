# Deep Dive — Distributed Payments System

## 1. Project Purpose

### Why This Project Was Built

This project demonstrates how to build a **production-grade payment processing system** that handles the fundamental challenges of distributed financial systems:

- **Data consistency** — Money must never be created or destroyed (double-entry invariant)
- **Exactly-once semantics** — A payment must not be processed twice, even across crashes and retries
- **Fault tolerance** — Infrastructure failures (Kafka, Redis, database) must not cause data loss
- **Fraud prevention** — Transactions must be scored in real-time before settlement
- **Observability** — Every transaction must be traceable end-to-end across service boundaries

### Real-World Problem Mapping

This maps directly to systems like Stripe, Square, or Adyen internally:

| Problem | Solution in This System |
|---------|------------------------|
| "Customer was charged twice" | Idempotency-Key + processed_events deduplication |
| "Payment succeeded but ledger shows nothing" | Outbox pattern — event + state in same DB TX |
| "Fraudulent transactions slipped through" | Two-stage pipeline: rules + ML scoring |
| "System crashed mid-payment" | Kafka redelivery + idempotent consumers |
| "Can't trace what happened to payment X" | Distributed tracing (correlationId + traceId in every hop) |
| "High traffic causes cascading failures" | Backpressure + circuit breakers + adaptive rate limiting |

---

## 2. System Design

### End-to-End Architecture

The system follows an **event-driven microservice architecture** with three main services communicating via Apache Kafka, backed by PostgreSQL for state and Redis for caching/rate-limiting.

```
HTTP Request
     │
     ▼
┌─────────────────────────────────────────────────────────────────┐
│                        API Gateway                               │
│  • JWT validation (HS256)                                       │
│  • Redis-backed rate limiting (100 req/s global, 50 req/s /v1)  │
│  • Circuit breaker (Resilience4j)                               │
│  • CORS configuration                                           │
│  • Request enrichment (X-Correlation-ID)                        │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Payment Service                            │
│                                                                 │
│  ┌──────────────┐   ┌───────────────┐   ┌────────────────┐    │
│  │ Rate Limiter │──▶│ Idempotency   │──▶│ Create Payment │    │
│  │ (Redis+      │   │ Check         │   │ (PENDING)      │    │
│  │  Caffeine)   │   │ (Redis+DB)    │   │                │    │
│  └──────────────┘   └───────────────┘   └───────┬────────┘    │
│                                                   │             │
│  ┌─────────────────────────────────────┐         │             │
│  │ Same DB Transaction                  │◀────────┘             │
│  │  • INSERT payments (PENDING)         │                       │
│  │  • INSERT outbox_events (NEW)        │                       │
│  └──────────────────────────────────────┘                       │
└─────────────────────────────────────────────────────────────────┘
                            │
         ┌──────────────────┼──────────────────────┐
         │    Outbox Publisher (Scheduled @1s)      │
         │  • Polls outbox_events WHERE status=NEW │
         │  • Publishes to Kafka                   │
         │  • Marks PUBLISHED or FAILED            │
         └──────────────────┬──────────────────────┘
                            │
                            ▼ payment.created
┌─────────────────────────────────────────────────────────────────┐
│                  PaymentEventConsumer                            │
│  • Reads payment.created                                        │
│  • Builds FraudRequestEvent                                     │
│  • Publishes to fraud.request (with trace headers)              │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼ fraud.request
┌─────────────────────────────────────────────────────────────────┐
│                    Fraud Service (Python)                        │
│                                                                 │
│  Stage 1: Rule-Based Checks                                     │
│  • Currency allowlist (USD, EUR, GBP, INR, JPY, CAD, AUD)      │
│  • Amount threshold (>$10,000 → reject)                        │
│  • Velocity check (>10 txns/60s per user → reject)             │
│                                                                 │
│  Stage 2: ML Scoring                                            │
│  • IsolationForest model (v1: 3 features, v2: 11 features)    │
│  • Score ≥ 0.75 → fraud                                       │
│                                                                 │
│  Output: Publishes to fraud.result                              │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼ fraud.result
┌─────────────────────────────────────────────────────────────────┐
│                   FraudResultConsumer                            │
│  • Idempotency guard (processed_events table)                   │
│  • If fraud=true → set FRAUD_REJECTED, emit fraud.alerts        │
│  • If fraud=false:                                              │
│    1. LedgerService.recordPayment() (double-entry)              │
│    2. AccountService.debit(payer) + credit(payee)               │
│    3. verifyLedgerBalance() (invariant check)                   │
│    4. Set status=SUCCESS, emit payment.processed                │
│  • On InsufficientBalanceException → set FAILED                 │
│  • On unrecoverable error → send to DLQ                        │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              WebSocket Broadcaster (Redis Pub/Sub)               │
│  • PaymentProcessedConsumer reads payment.processed             │
│  • Broadcasts to all connected WebSocket clients                │
│  • Supports multi-instance via Redis pub/sub fan-out            │
└─────────────────────────────────────────────────────────────────┘
```

### Data Flow (Step-by-Step)

1. Client sends `POST /v1/payments` with `Idempotency-Key` header and JWT bearer token
2. API Gateway validates JWT, applies rate limit, adds `X-Correlation-ID`
3. Payment Service rate-limits per userId (Redis + Caffeine fallback)
4. Checks idempotency: Redis cache → DB pessimistic lock lookup
5. Creates Payment entity (PENDING) + OutboxEvent in single DB transaction
6. Returns `202 PENDING` immediately to client
7. Outbox Publisher (scheduler, every 1s) polls NEW outbox events
8. Publishes `payment.created` to Kafka with trace headers (eventId, correlationId, traceId)
9. PaymentEventConsumer transforms into `FraudRequestEvent`, publishes to `fraud.request`
10. Fraud Service (Python Kafka consumer) scores the transaction
11. Publishes `fraud.result` to Kafka
12. FraudResultConsumer processes result:
    - Dedup check via `processed_events` table (atomic with business logic)
    - If clean: ledger write → account balance update → status=SUCCESS
    - If fraud: status=FRAUD_REJECTED → emit alert
13. WebSocket broadcasts final status to frontend dashboard

---

## 3. Tech Stack Justification

| Technology | Why Chosen | Alternatives Considered |
|-----------|-----------|------------------------|
| **Spring Boot 3.4** | Mature ecosystem, Spring Kafka/Data/Security integration, production-proven | Quarkus (faster startup but less ecosystem), Micronaut |
| **Spring Cloud Gateway** | Native reactive gateway, integrates with Redis rate limiter, circuit breaker built-in | Kong (operational overhead), Envoy (more complex config) |
| **PostgreSQL 15** | ACID transactions essential for financial data, strong ecosystem, JSONB for flexibility | MySQL (weaker isolation), CockroachDB (distributed but added complexity) |
| **Apache Kafka** | Durable event log, exactly-once semantics, partitioning for scale, replay capability | RabbitMQ (no durable log replay), AWS SQS (vendor lock-in) |
| **Redis 7** | Sub-ms latency for rate limiting, Lua atomicity, pub/sub for WebSocket fan-out | Memcached (no Lua, no pub/sub), Hazelcast (heavier) |
| **FastAPI (Python)** | ML-friendly ecosystem (sklearn, numpy), async support, auto-generated docs | Flask (sync-only), Java ML (limited sklearn equivalents) |
| **React + TypeScript** | Type safety for complex event models, excellent dev tooling, Vite for fast builds | Vue (smaller ecosystem), Angular (heavier for a dashboard) |
| **IsolationForest** | Unsupervised (no labeled fraud data needed), fast inference, good for anomaly detection | XGBoost (needs labels), Autoencoder (more complex) |
| **Resilience4j** | Lightweight, annotation-driven, no external dependencies | Hystrix (deprecated), Sentinel (Alibaba-specific) |
| **Docker Compose** | Single-command full stack, reproducible, good for local dev | Kubernetes (overkill for local), Podman (less tooling) |

---

## 4. Core Modules Breakdown

### Payment Service — `com.payments.payment_service`

#### PaymentService (service/)
- **Responsibility:** Orchestrates payment lifecycle (create, process result)
- **Key Methods:** `createPayment()`, `processPaymentResult()`
- **Patterns:** Transactional Outbox, Idempotent Write, @Retryable with exponential backoff
- **Design:** Single @Transactional boundary ensures atomicity of payment + outbox event creation

#### LedgerService (service/)
- **Responsibility:** Double-entry bookkeeping with invariant enforcement
- **Key Methods:** `recordPayment()`, `verifyLedgerBalance()`
- **Patterns:** Double-Entry Accounting, Optimistic Locking (via AccountService)
- **Design:** Every payment creates exactly one DEBIT + one CREDIT entry; invariant validated post-insert

#### OutboxPublisherService (service/)
- **Responsibility:** Reliable event publishing via transactional outbox
- **Key Methods:** `createOutboxEvent()`, `publishPendingEvents()` (scheduled)
- **Patterns:** Transactional Outbox, Polling Publisher
- **Design:** Polls every 1s, batch of 50, marks PUBLISHED on Kafka ack, FAILED after 3 retries

#### RateLimiterService (service/)
- **Responsibility:** Per-user rate limiting with adaptive backpressure
- **Key Methods:** `checkRateLimit()`
- **Patterns:** Fixed-Window Counter (Redis Lua), Graceful Degradation (Caffeine fallback)
- **Design:** Integrates with BackpressureService to reduce limits under Kafka lag (50% at critical, 90% at severe)

#### BackpressureService (service/)
- **Responsibility:** Kafka consumer lag monitoring + system-level throttle signals
- **Key Methods:** `isBackpressureActive()`, `getRateLimitMultiplier()`
- **Patterns:** Adaptive Throttling, Health Indicator
- **Design:** Polls AdminClient every 5s, exposes lag via Prometheus gauge, signals rate limiter

#### IdempotentConsumerService (service/)
- **Responsibility:** Exactly-once consumer deduplication
- **Key Methods:** `tryMarkProcessed()`
- **Patterns:** Idempotent Receiver, Transactional Dedup
- **Design:** INSERT into processed_events within caller's @Transactional; PK violation = duplicate

#### FraudResultConsumer (kafka/)
- **Responsibility:** Consumes fraud decisions, finalizes payment status
- **Key Methods:** `handleFraudResult()`
- **Patterns:** Manual Ack, DLQ Routing, MDC-based Tracing
- **Design:** Maps fraud.result → processPaymentResult(); unrecoverable errors → fraud.result.DLQ

#### PaymentEventConsumer (kafka/)
- **Responsibility:** Bridges payment.created → fraud.request
- **Key Methods:** `handlePaymentCreated()`
- **Patterns:** Event Translator, Header Propagation
- **Design:** Transforms PaymentEvent to FraudRequestEvent; propagates all trace headers

#### WebhookService (service/)
- **Responsibility:** Delivers payment events to merchant-registered HTTPS endpoints
- **Key Methods:** `dispatchEvent()`, `retryFailedDeliveries()` (scheduled)
- **Patterns:** Exponential Backoff, HMAC Signing, Idempotent Delivery
- **Design:** Async delivery, exponential backoff (5s → 30s → 120s → 600s → 1800s), DLQ after max attempts

### API Gateway — `com.payments.gateway`

#### SecurityConfig
- **Responsibility:** JWT resource server configuration
- **Design:** HS256 symmetric key validation; `/actuator/**` and health endpoints permitted without auth

#### RateLimiterConfig
- **Responsibility:** Provides Redis-based rate limiter key resolver
- **Design:** Uses remote address (not X-Forwarded-For to prevent spoofing)

#### RequestValidationFilter
- **Responsibility:** Input validation at gateway level
- **Design:** Regex-based path matching, validates request structure before forwarding

### Fraud Service — Python

#### ModelManager (model_manager.py)
- **Responsibility:** ML model lifecycle (train, score, version, hot-reload)
- **Design:** Thread-safe atomic model swap; v1 (3 features) and v2 (11 features) support

#### FeatureExtractor (feature_engineering.py)
- **Responsibility:** Transforms raw transaction into feature vector for ML scoring
- **Design:** Produces FeatureVector with `to_basic_array()` (v1) and `to_array()` (v2)

#### FeatureStore (feature_store.py)
- **Responsibility:** Redis-backed real-time feature computation (user stats, velocity)
- **Design:** Falls back to in-process computation when Redis is unavailable

#### Kafka Consumer Loop (main.py)
- **Responsibility:** Async fraud pipeline — consumes fraud.request, produces fraud.result
- **Design:** Multiple worker threads (configurable), each with own Consumer instance (confluent-kafka is not thread-safe)

---

## 5. Critical Workflows

### Payment Creation Flow

```
1. POST /v1/payments {userId, merchantId, amount, currency}
   │
2. API Gateway: JWT validate → rate limit check → add X-Correlation-ID
   │
3. PaymentController.createPayment()
   │
4. CorrelationIdFilter: MDC.put(correlationId, traceId)
   │
5. PaymentService.createPayment() [@Transactional(READ_COMMITTED)]
   │
   ├── 5a. RateLimiterService.checkRateLimit(userId)
   │        → Redis INCR + EXPIRE (Lua atomic)
   │        → If over limit: throw RateLimitExceededException (429)
   │
   ├── 5b. Idempotency check #1: Redis cache lookup
   │        → Hit: return cached PaymentResponse
   │
   ├── 5c. Idempotency check #2: DB pessimistic lock (SELECT FOR UPDATE)
   │        → Hit: cache in Redis, return PaymentResponse
   │
   ├── 5d. Create Payment entity (status=PENDING)
   │
   ├── 5e. OutboxPublisherService.createOutboxEvent("payment.created")
   │        → INSERT into outbox_events (same TX!)
   │
   ├── 5f. Cache idempotency key in Redis
   │
   └── 5g. Return PaymentResponse (status=PENDING)
```

### Fraud Check + Settlement Flow

```
1. OutboxPublisherService (scheduled @1s)
   → SELECT outbox_events WHERE status=NEW LIMIT 50
   → Publish to Kafka topic (e.g., "payment.created")
   → Mark event PUBLISHED
   │
2. PaymentEventConsumer.handlePaymentCreated()
   → Build FraudRequestEvent
   → Publish to "fraud.request" with headers (eventId, correlationId, traceId)
   │
3. Fraud Service Kafka Consumer
   → Deserialize event
   → Stage 1: Rule checks (currency, amount, velocity)
   → Stage 2: ML scoring (IsolationForest)
   → Publish result to "fraud.result"
   │
4. FraudResultConsumer.handleFraudResult()
   │
   ├── 4a. IdempotentConsumerService.tryMarkProcessed(eventId)
   │        → INSERT processed_events (PK constraint = dedup)
   │        → false? Skip (already processed)
   │
   ├── 4b. Load Payment from DB
   │        → Status guard: if != PENDING → skip
   │
   ├── 4c. IF fraud=true:
   │        → status = FRAUD_REJECTED
   │        → Emit fraud.alerts + payment.failed via outbox
   │
   └── 4d. IF fraud=false:
            → LedgerService.recordPayment() [double-entry]
            → AccountService.debit(payer) + credit(payee)
            → LedgerService.verifyLedgerBalance()
            → status = SUCCESS
            → Emit payment.processed via outbox
```

---

## 6. Performance Considerations

### Latency-Sensitive Areas

| Path | Target Latency | Optimization |
|------|---------------|--------------|
| POST /payments (API response) | <100ms p95 | Returns PENDING immediately; fraud check is async |
| Idempotency cache hit | <5ms | Redis GET before DB query |
| Rate limit check | <2ms | Single Redis Lua script (atomic INCR+EXPIRE) |
| Fraud rule checks | <1ms | In-memory, no I/O |
| ML scoring | <10ms | Preloaded IsolationForest, numpy vectorized |
| WebSocket broadcast | <50ms | Redis pub/sub, no DB involved |

### Bottlenecks

1. **Outbox polling interval (1s)** — Adds 0-1s latency between payment creation and Kafka publish. Acceptable trade-off for guaranteed delivery.
2. **PostgreSQL connection pool (20 max)** — Under extreme load, connection exhaustion causes queueing. HikariCP's 30s timeout prevents indefinite blocking.
3. **Kafka partition count** — Throughput ceiling = number of partitions × consumer throughput per partition. Default topics may need explicit partition count tuning.
4. **ML model inference** — IsolationForest is O(n_estimators × log(n_samples)). With 100 trees this is ~1ms but grows with ensemble size.

### Optimizations Implemented

- **Kafka producer batching** — `linger.ms=5`, `batch.size=32768` reduces network calls
- **Hibernate batch inserts** — `jdbc.batch_size=50` with `order_inserts=true`
- **Redis connection pooling** — Lettuce pool (min 5, max 20 connections)
- **JPA open-in-view disabled** — Prevents accidental N+1 queries in controllers
- **Caffeine rate limit fallback** — Bounded cache (50K entries, 2min TTL) prevents heap exhaustion

---

## 7. Scalability Design

### Horizontal Scaling Strategy

| Component | Scaling Method |
|-----------|---------------|
| Payment Service | Add pods; stateless (state in PostgreSQL/Redis) |
| Fraud Service | Add pods + increase Kafka partitions on fraud.request |
| API Gateway | Add instances; Redis rate limiter is shared state |
| PostgreSQL | Read replicas for queries; PgBouncer for connection pooling |
| Kafka | Add brokers + partitions; rebalance consumers automatically |
| Redis | Redis Cluster for sharding; Sentinel for HA |

### Stateless/Stateful Decisions

- **Stateless:** Payment Service, API Gateway, Fraud Service HTTP layer
- **Stateful:** PostgreSQL (source of truth), Redis (rate limits, cache), Kafka (event log)
- **Design principle:** All application services are stateless and disposable; horizontal scaling is trivial

### Kafka Partition Strategy

The system uses **composite partition keys** to balance load:

```
partitionKey = userId + ":" + (sequenceNum % SALT_FACTOR)
```

- Without salt: A high-volume user sends all events to one partition → hot partition
- With salt (factor=3): Events are spread across 3 partitions → even distribution
- Trade-off: Loses strict per-user ordering, but fraud scoring is order-independent

### Caching Strategy

| Cache | TTL | Purpose |
|-------|-----|---------|
| Idempotency key → paymentId | 24h | Avoid DB lookup for repeat requests |
| Payment object | 5min | Reduce DB reads for GET /payments/{id} |
| Rate limit counters | 60s (fixed window) | Atomic per-user request counting |
| Velocity tracking (fraud) | 60s | Per-user transaction count for rule engine |

---

## 8. Failure Handling

### Edge Cases Handled

| Scenario | Handling |
|----------|----------|
| Kafka down during payment creation | Outbox event stays in DB; publisher retries when Kafka recovers |
| Redis down during rate limiting | Falls back to Caffeine in-memory counter (per-JVM, bounded) |
| DB connection timeout | @Retryable with exponential backoff (200ms, 400ms, 800ms) |
| Fraud service unreachable | Circuit breaker opens after 5 failures; auto-recovers in 10s |
| Duplicate Kafka delivery | processed_events PK constraint prevents double processing |
| Payment already processed (crash recovery) | Status guard: if status != PENDING → skip |
| Ledger write fails (insufficient balance) | InsufficientBalanceException → FAILED status, no partial ledger |
| Poison message (unparseable) | Caught in consumer, sent to DLQ, acknowledged |
| Webhook endpoint unreachable | Exponential backoff retry (5 attempts), then marked FAILED |
| Concurrent idempotency key | DB pessimistic lock (SELECT FOR UPDATE) serializes access |

### Retry Mechanisms

| Layer | Mechanism | Config |
|-------|-----------|--------|
| Service method | @Retryable (Spring Retry) | 3 attempts, 200ms×2 backoff, TransientDataAccessException |
| Kafka consumer | Spring Kafka ExponentialBackOff | Configured in KafkaConfig |
| Kafka producer | Idempotent producer, 5 retries | `enable.idempotence=true` |
| Outbox publisher | Retry count on OutboxEvent entity | 3 max retries before FAILED |
| Webhook delivery | Scheduled retry with exponential backoff | 5 attempts: 5s, 30s, 120s, 600s, 1800s |
| Circuit breaker | Auto half-open after wait duration | 10s wait, 3 calls in half-open |

### Error Handling Strategy

1. **Transient errors** → Retry with backoff (DB timeouts, network blips)
2. **Business errors** → Return meaningful error response (insufficient balance, rate limit)
3. **Poison messages** → DLQ routing + alert (unrecoverable deserialization failures)
4. **Infrastructure outages** → Graceful degradation (Redis → Caffeine, Kafka → outbox holds)

---

## 9. Testing Strategy

### Current Test Coverage

| Layer | Implementation | Status |
|-------|---------------|--------|
| Context Load Test | `PaymentServiceApplicationTests` | Verifies Spring context boots |
| Load Tests | k6 scripts (4 profiles) | default, stress, soak, spike |
| Failure Simulations | Shell scripts in `scripts/failure-simulation/` | Kafka, Redis, DB, fraud, network |
| Manual/Postman | Documented in `docs/postman-testing-guide.md` | End-to-end API testing |

### Load Test Profiles

| Profile | Description | VUs | Duration |
|---------|-------------|-----|----------|
| default | Ramp 10→100→10 | 100 peak | ~2.5min |
| stress | Sustained 1000+ TPS | 1500 peak | ~5.5min |
| soak | Sustained load for memory leaks | 200 | 34min |
| spike | Instant 2000 VU spike | 2000 peak | ~1.5min |

### Failure Simulation Scripts

- `database-failure.sh` — Pauses/resumes PostgreSQL container
- `kafka-failure.sh` — Stops Kafka broker, observes outbox buffering
- `redis-failure.sh` — Kills Redis, validates Caffeine fallback
- `fraud-service-failure.sh` — Stops fraud service, validates circuit breaker
- `network-partition.sh` — Introduces network latency/packet loss via `tc`

### Testing Gaps

- No unit tests for individual service classes (PaymentService, LedgerService)
- No integration tests with embedded Kafka/PostgreSQL (TestContainers)
- No contract tests between services (API schema validation)
- No mutation testing

---

## 10. Deployment Model

### Docker Compose (Development/Demo)

All services run as containers orchestrated by `docker-compose.yml`:

- **Profiles:** No profile = infrastructure only; `app`/`full` = infrastructure + application services
- **Health checks:** Every service has a Docker healthcheck with dependency ordering
- **Volumes:** Persistent data for PostgreSQL, Redis, Prometheus, Grafana
- **Networking:** Internal Docker network; services reference each other by container name

### Container Strategy

| Service | Base Image | Multi-Stage | Non-Root |
|---------|-----------|-------------|----------|
| Payment Service | eclipse-temurin:17-jre-alpine | Yes (build + runtime) | Yes (appuser) |
| API Gateway | eclipse-temurin:17-jre-alpine | Yes (build + runtime) | Yes (appuser) |
| Fraud Service | python:3.11-slim | No (single stage) | No (default user) |
| Frontend | nginx:alpine | Yes (node build + nginx) | nginx default |

### Production Deployment Path (Not Yet Implemented)

For production, the system would require:
- Kubernetes with separate Deployments per service
- Helm charts for configuration management
- Secrets management (Vault/AWS Secrets Manager) replacing env var credentials
- PostgreSQL managed service (RDS/Cloud SQL) with `ddl-auto: validate`
- Kafka managed service (Confluent Cloud/MSK) with proper ACLs
- Redis Cluster or managed ElastiCache
- CI/CD pipeline (GitHub Actions → Docker build → push to registry → K8s rollout)

### Monitoring Stack

- **Prometheus** (port 9090) — Scrapes `/actuator/prometheus` (Java) and `/metrics` (Python) every 15s
- **Grafana** (port 3000) — Auto-provisioned datasource + "Payments System Overview" dashboard (10 panels)
- **Structured Logging** — JSON format via logstash-logback-encoder, queryable in ELK/Loki

---

## 11. Interview-Focused Q&A

### "Explain this project in 1 minute"

> This is a distributed payment processing system built with Spring Boot, Kafka, and PostgreSQL. When a payment comes in, it goes through an API gateway for auth and rate limiting, then gets persisted with an outbox event in a single database transaction. An outbox publisher sends the event to Kafka, which triggers an async fraud check via a Python ML service. Once the fraud result comes back, the payment is either settled with a double-entry ledger write or rejected. The system guarantees exactly-once processing through idempotent consumers, handles infrastructure failures gracefully via circuit breakers and fallback mechanisms, and provides real-time visibility through WebSocket-powered dashboards and Prometheus metrics.

### "Biggest challenges faced"

1. **Exactly-once semantics across Kafka redeliveries** — Solved by combining an `IdempotentConsumerService` (processed_events table with PK constraint) inside the same `@Transactional` boundary as the business logic. If the TX commits, both the payment status AND the dedup record are persisted atomically. If it crashes before commit, both roll back, and Kafka safely redelivers.

2. **Outbox pattern correctness** — The key insight is that the outbox event must be written in the SAME database transaction as the payment state change. Separate transactions create a window where the payment exists but the event doesn't (or vice versa). The publisher then polls and publishes idempotently.

3. **Preventing negative ledger balances** — `LedgerService.recordPayment()` uses `@Transactional(noRollbackFor = InsufficientBalanceException.class)` so that a balance check failure propagates cleanly to the caller without poisoning the outer transaction's rollback-only flag.

4. **Kafka consumer lag causing cascading failure** — Without backpressure, if consumers fall behind, producers keep adding to the queue indefinitely. The `BackpressureService` monitors lag and dynamically reduces the rate limit (50% at critical lag, 90% at severe), letting consumers catch up.

### "What would you improve?"

1. **Replace fixed-window rate limiter with sliding window** — Current implementation uses INCR+EXPIRE which allows burst at window boundaries. A sorted-set sliding window would provide smoother throttling.
2. **Add comprehensive unit and integration tests** — Currently only has a context load test. Need TestContainers-based integration tests and unit tests with mocked dependencies.
3. **Schema Registry for Kafka events** — Currently uses raw JSON with no schema evolution guarantees. Avro + Schema Registry would catch breaking changes at compile time.
4. **Separate read/write models (CQRS)** — Payment queries hit the same PostgreSQL as writes. Under high read volume, a denormalized read model (Elasticsearch or materialized views) would reduce DB pressure.
5. **Asymmetric JWT (RS256)** — Current HS256 requires the same secret in both gateway and issuer, making rotation harder.

### "How would you scale this to 1M users?"

1. **Kafka partitions** — Increase `fraud.request` and `payment.created` partitions to 48-96. Each consumer instance handles a subset of partitions. Add Fraud Service pods to match.
2. **PostgreSQL sharding** — Shard by `userId` using Citus or application-level sharding. Each shard handles ~100K users.
3. **Redis Cluster** — Move from standalone Redis to a cluster (6+ nodes) for rate limiting and caching. Shards by key hash.
4. **Connection pooling** — Add PgBouncer in front of PostgreSQL (transaction pooling mode). Increase HikariCP pool size per pod.
5. **Read replicas** — Route GET queries to PostgreSQL read replicas. Write path stays on primary.
6. **CDN + Edge** — Serve frontend via CDN. API Gateway deployed in multiple regions with GeoDNS.
7. **Outbox optimization** — Replace polling with CDC (Change Data Capture via Debezium) for lower latency event publishing.
8. **At 1M users with 10 TPS average** = 10M TPS peak. This requires:
   - Multiple Kafka clusters or dedicated topics per region
   - Sharded PostgreSQL (32+ shards)
   - Stateless services auto-scaled to 50+ pods
   - Global load balancer with regional failover

### "Design trade-offs you made"

| Decision | Trade-off |
|----------|-----------|
| Async fraud (Kafka) vs sync HTTP | **Chose async:** Higher latency (1-2s vs 100ms) but decoupled, retryable, and scalable. Fraud service downtime doesn't block payments. |
| Fixed-window vs sliding-window rate limit | **Chose fixed-window:** Simpler implementation, single Redis INCR command. Accepts 2× burst at window boundary. |
| Outbox polling (1s) vs CDC | **Chose polling:** No additional infrastructure (Debezium + Kafka Connect). Adds 0-1s latency which is acceptable for async flow. |
| HS256 JWT vs RS256 | **Chose HS256:** Single shared secret, simpler setup. Trade-off: rotation requires coordinated restart of gateway + issuer. |
| In-process ML model vs external service | **Chose in-process:** Sub-10ms inference, no network hop. Trade-off: model updates require service restart (mitigated by hot-reload endpoint). |
| PostgreSQL for everything vs polyglot persistence | **Chose single DB:** Operational simplicity, strong consistency. Trade-off: read scalability limited (mitigated by Redis cache). |
| Manual Kafka offset commit vs auto-commit | **Chose manual:** Exactly-once semantics require processing before ack. Trade-off: slightly higher latency per message. |

---

## 12. Possible Extensions

1. **Payment Methods** — Add card tokenization (PCI DSS), bank transfers (ACH/SEPA), and digital wallets via Strategy pattern
2. **Saga Orchestrator** — Multi-step payment workflows (authorize → capture → settle) with compensation logic for partial failures
3. **A/B Testing for Fraud Models** — Deploy multiple models simultaneously in shadow mode, compare precision/recall before promotion
4. **Real-Time Fraud Dashboard** — Grafana alerting on fraud rate spikes, geographic anomaly detection
5. **Audit Trail** — Append-only event store for compliance (PCI, SOX), queryable by regulators
6. **Multi-Currency Support** — FX rate service integration, conversion at settlement time, multi-currency ledger accounts
7. **Merchant Portal** — Self-service webhook management, payment analytics, settlement reports
8. **Idempotency Key Expiration** — TTL-based cleanup of old idempotency keys with configurable retention
9. **Blue-Green Deployments** — Zero-downtime deployment with Kafka consumer group rebalancing
10. **GraphQL API** — Alternative query interface for frontend flexibility (aggregations, filtering)
11. **Event Sourcing** — Replace mutable payment state with immutable event log; derive current state via projection
12. **Machine Learning Pipeline** — Feature store with historical data, model training pipeline (MLflow), automated retraining on drift detection
