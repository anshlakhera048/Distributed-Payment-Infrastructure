# Architecture — Distributed Payments Infrastructure with Real-Time Fraud Detection

> **Audience**: Senior engineers, system design interviewers, infrastructure teams.
> **Depth**: Production-grade — covers implementation details, trade-offs, failure modes, and scaling strategies grounded in the actual codebase.

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [High-Level Architecture](#2-high-level-architecture)
3. [Detailed Service Design](#3-detailed-service-design)
4. [Data Modeling](#4-data-modeling)
5. [Kafka / Event Streaming Design](#5-kafka--event-streaming-design)
6. [Distributed Systems Patterns](#6-distributed-systems-patterns)
7. [Fraud Detection System](#7-fraud-detection-system)
8. [Consistency Model](#8-consistency-model)
9. [Failure Handling](#9-failure-handling)
10. [Performance & Scaling](#10-performance--scaling)
11. [Observability](#11-observability)
12. [Security](#12-security)
13. [DevOps & Deployment](#13-devops--deployment)
14. [Trade-offs and Alternatives](#14-trade-offs-and-alternatives)
15. [Interview Talking Points](#15-interview-talking-points)
16. [What Could Be Improved Further](#16-what-could-be-improved-further)

---

## 1. System Overview

### 1.1 System Goals

This system is a Stripe/Adyen-class payment processing platform that accepts payment requests, runs real-time fraud detection (rule engine + ML), records double-entry ledger transactions, and delivers webhook notifications — all in an event-driven, exactly-once-safe architecture.

Core capabilities:

| Capability | Description |
|---|---|
| **Payment Ingestion** | Accept payment requests via REST API with idempotency guarantees |
| **Real-Time Fraud Detection** | Two-stage pipeline: deterministic rule engine → IsolationForest ML scoring |
| **Double-Entry Ledger** | Every successful payment produces balanced DEBIT + CREDIT entries with invariant enforcement |
| **Event-Driven Processing** | Kafka-backed async pipeline with outbox pattern for DB↔broker consistency |
| **Webhook Delivery** | Merchant notification system with exponential backoff retry and idempotent delivery |
| **Observability** | Structured JSON logging, distributed tracing, Prometheus metrics, Grafana dashboards |
| **Reconciliation** | Scheduled job detects stale payments, ghost successes, ledger imbalances, orphan entries |

### 1.2 Non-Functional Requirements

| Requirement | Target | Implementation |
|---|---|---|
| **Availability** | 99.95% (≈22 min downtime/month) | Circuit breakers, fail-open fraud fallback, Redis failover to Caffeine |
| **Latency (p95)** | POST /payments < 500ms, GET < 200ms | Redis caching, connection pooling, async Kafka pipeline |
| **Throughput** | ≥ 1,000 TPS sustained, burst to 5,000 | HikariCP (20 pool), Kafka 3-partition concurrency, Redis rate limiting |
| **Consistency** | Ledger: strong. Events: exactly-once semantics. Cache: eventual | Transactional outbox, `processed_events` dedup table, double-entry invariant checks |
| **Durability** | Zero payment loss | `acks=all` Kafka producer, PostgreSQL WAL, outbox retry (3 attempts), DLQ routing |
| **Idempotency** | Any request replayable without side effects | Redis cache → DB `UNIQUE` constraint → `SELECT ... FOR UPDATE` pessimistic lock |
| **Scalability** | Horizontal scaling at every tier | Stateless services, Kafka partitioning, Redis cluster-ready, DB read replicas |

### 1.3 Assumptions

| Assumption | Value | Rationale |
|---|---|---|
| Peak TPS | 5,000 payments/sec | Sized for a mid-to-large fintech (Stripe processes ~10K TPS globally) |
| Average payload | ~500 bytes JSON | UUID-based IDs, single-currency amounts, short descriptions |
| Fraud latency budget | < 200ms per check | IsolationForest inference ~1-5ms; Kafka round-trip dominates |
| Event retention | 7 days (Kafka), indefinite (DB) | Kafka `log.retention.hours=168`; DB for audit/compliance |
| Currency set | 7 currencies (USD, EUR, GBP, INR, JPY, CAD, AUD) | Allowlisted in fraud service; unknown currencies → auto-reject |
| Failure mode | Any single service can crash independently | Designed for partial failure — no 2PC, saga-based recovery |

---

## 2. High-Level Architecture

### 2.1 Architecture Diagram

```
                              ┌─────────────────────────────────────────┐
                              │     Client / k6 Load Test / Frontend    │
                              └──────────────┬──────────────────────────┘
                                             │ HTTPS + JWT + Idempotency-Key
                                             ▼
                              ┌─────────────────────────────────────────┐
                              │    API Gateway  (Spring Cloud · :8090)  │
                              │    JWT auth · Redis rate limit · CB     │
                              │    RequestEnrichmentFilter              │
                              │    (injects X-Correlation-ID, X-User-Id)│
                              └──────────────┬──────────────────────────┘
                                             │ HTTP (stripped auth header)
                                             ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                    Payment Service  (Spring Boot 3.4.4 · :8080)                  │
│                                                                                  │
│  ┌───────────────┐    ┌──────────────┐    ┌───────────────┐    ┌──────────────┐  │
│  │PaymentController│──▶│PaymentService │──▶│OutboxPublisher│──▶│  Kafka       │  │
│  │POST /payments  │    │@Transactional │    │@Scheduled 1s  │    │  Producer   │  │
│  │GET  /payments  │    │              │    │top50 per poll │    │  acks=all   │  │
│  └───────────────┘    └──────┬───────┘    └───────────────┘    └──────────────┘  │
│                              │                                                   │
│  ┌───────────────────────────┼───────────────────────────────────────────────┐    │
│  │             Kafka Consumers (3 concurrency, manual ack)                   │    │
│  │  PaymentEventConsumer ◀── payment.created → fraud.request                │    │
│  │  FraudResultConsumer  ◀── fraud.result   → processPaymentResult()        │    │
│  │  PaymentProcessedConsumer ◀── payment.processed / payment.failed         │    │
│  │                               → WebhookService + WebSocketBroadcaster    │    │
│  └───────────────────────────────────────────────────────────────────────────┘    │
│                                                                                  │
│  ┌─────────────┐  ┌────────────────┐  ┌───────────────────┐  ┌───────────────┐  │
│  │LedgerService │  │ReconcilService │  │IdempotentConsumer │  │  PaymentCache │  │
│  │double-entry  │  │@Cron 30min     │  │PK-based dedup     │  │  Redis+evict  │  │
│  └─────────────┘  └────────────────┘  └───────────────────┘  └───────────────┘  │
│                                                                                  │
│  PostgreSQL: payments · outbox_events · processed_events · ledger_entries         │
│              webhook_endpoints · webhook_deliveries                               │
│  Redis: rate_limit:* · idem:* · payment:* · ws:payments pub/sub                  │
└──────────────────────────────────────────────────────────────────────────────────┘
                         │ Kafka: fraud.request
                         ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                    Fraud Service  (FastAPI · Python 3.11 · :8000)                │
│                                                                                  │
│  Stage 1: Rule Engine (velocity >10 tx/60s, amount >$10K, currency allowlist)    │
│  Stage 2: IsolationForest ML (features: amount, is_high_value, currency_risk)    │
│  3 Kafka consumer worker threads · confluent-kafka · Redis velocity tracking     │
│  Prometheus: fraud_requests_total, fraud_request_duration_seconds                │
└──────────────────────────────────────────────────────────────────────────────────┘
                         │ Kafka: fraud.result
                         └──▶ FraudResultConsumer (payment-service)

┌───────────────────┐   ┌───────────────────┐   ┌───────────────────┐
│  Prometheus :9090  │   │   Grafana :3000    │   │  Frontend :5173   │
│  15s scrape        │   │   dashboards       │   │  React + WS      │
│  payment-service   │◀──│                    │   │  /ws/payments     │
│  fraud-service     │   │                    │   │                   │
└───────────────────┘   └───────────────────┘   └───────────────────┘
```

### 2.2 Data Flow — Step by Step

**Happy path for a single payment:**

```
1. Client → POST /api/payments (JWT + Idempotency-Key header)
2. API Gateway:
   a. Validate JWT (HS256, local verification)
   b. Redis rate limit check (100 rps global, 50 rps for /api/payments)
   c. Inject X-Correlation-ID + X-User-Id headers
   d. Strip Authorization header
   e. Forward to payment-service:8080/payments
3. Payment Service (PaymentController → PaymentService.createPayment):
   a. RateLimiterService.checkRateLimit(userId)  — Redis Lua INCR+EXPIRE, Caffeine fallback
   b. PaymentCacheService.getPaymentIdForIdempotencyKey(key)  — Redis first
   c. If miss: PaymentRepository.findByIdempotencyKeyForUpdate(key)  — SELECT ... FOR UPDATE
   d. If new: save Payment(status=PENDING) + OutboxEvent(topic=payment.created) in SAME TX
   e. Cache idempotency key + payment response in Redis
   f. Return 201 Created with PaymentResponse
4. OutboxPublisherService (@Scheduled every 1s):
   a. SELECT top 50 FROM outbox_events WHERE status='NEW' ORDER BY created_at
   b. Build ProducerRecord with Kafka headers (x-event-id, x-correlation-id, x-trace-id)
   c. kafkaTemplate.send() with partition key = userId
   d. On success callback: mark outbox event as PUBLISHED
   e. On failure: increment retry_count; after max_retries → mark FAILED
5. Kafka topic: payment.created → PaymentEventConsumer:
   a. Extract trace headers from Kafka record
   b. Build FraudRequestEvent (payment_id, user_id, amount, currency)
   c. Publish to fraud.request topic (propagate all trace headers)
   d. Manual ack
6. Fraud Service (Kafka consumer worker thread):
   a. Deserialize fraud.request message
   b. Stage 1 — Rule engine: currency allowlist → amount threshold → velocity check
   c. Stage 2 — ML: IsolationForest.decision_function() → normalize to [0,1]
   d. Produce fraud.result to Kafka with x-trace-id header forwarding
   e. Synchronous commit
7. Kafka topic: fraud.result → FraudResultConsumer:
   a. Deserialize FraudResultEvent
   b. Call paymentService.processPaymentResult() — @Transactional, @Retryable (3x)
   c. Inside TX:
      i.   IdempotentConsumerService.tryMarkProcessed() — PK insert dedup
      ii.  If fraud=true: update status → FRAUD_REJECTED, write fraud.alerts + payment.failed outbox
      iii. If fraud=false:
           - LedgerService.recordPayment() — 2 entries (DEBIT merchant → CREDIT payer)
           - LedgerService.verifyLedgerBalance() — SUM(DEBIT) == SUM(CREDIT) check
           - Update status → SUCCESS
           - Write payment.processed outbox event
      iv.  Evict Redis cache
   d. Manual ack
8. Kafka topic: payment.processed → PaymentProcessedConsumer:
   a. PaymentNotificationService.dispatchNotification() — @Transactional
      i.   IdempotentConsumerService.tryMarkProcessed() — dedup
      ii.  WebhookService.dispatchEvent() — @Async, find endpoints, POST with HMAC
   b. WebSocketBroadcaster.broadcast() — fire-and-forget (outside TX)
   c. Manual ack
```

### 2.3 Sync vs Async Boundaries

| Boundary | Type | Why |
|---|---|---|
| Client → API Gateway → Payment Service | **Synchronous** (HTTP) | Client needs immediate 201 response with payment ID |
| Payment creation → Kafka (via outbox) | **Asynchronous** | Decouples write-path latency from downstream processing |
| payment.created → Fraud Service | **Asynchronous** (Kafka) | ML scoring is CPU-bound; async allows backpressure |
| fraud.result → Ledger + Status Update | **Asynchronous** (Kafka) | Keeps fraud service stateless; retry handled by consumer |
| payment.processed → Webhook Delivery | **Asynchronous** (Kafka + @Async) | Webhook targets are external, unreliable; must not block pipeline |
| WebSocket broadcast | **Fire-and-forget** | Real-time UI push; outside transaction boundary to prevent rollback from UI failure |
| Fraud HTTP fallback (circuit breaker) | **Synchronous** (backup path) | Direct HTTP call when Kafka pipeline needs bypass |

---

## 3. Detailed Service Design

### 3.1 API Gateway (Spring Cloud Gateway — :8090)

**Responsibilities:**
- JWT authentication (HS256 symmetric key, local validation — no auth server roundtrip)
- Redis-backed token-bucket rate limiting (global: 100 rps / burst 200; payments: 50 rps / burst 100)
- Request enrichment: injects `X-Correlation-ID`, `X-User-Id` (from JWT `sub` claim), strips `Authorization` header
- Circuit breaker on payment-service (opens after failure threshold, forwards to `FallbackController`)
- Retry filter: 3 retries on 502/503/504 for GET requests (exponential backoff 100ms → 500ms)

**APIs:**

| Route | Target | Filters |
|---|---|---|
| `/api/payments/**` | `payment-service:8080/payments/**` | StripPrefix=1, RateLimiter, Retry, CircuitBreaker |
| `/management/**` | `payment-service:8080/management/**` | None (internal) |
| `/actuator/health` | Self | No auth required |

**Request Enrichment Filter (Order -1 — runs before routing):**
```
Incoming request
  ├─ Extract JWT subject claim → X-User-Id header
  ├─ Read or generate X-Correlation-ID
  ├─ Remove Authorization header (downstream trusts X-User-Id only)
  └─ Forward mutated request
```

**Failure Scenarios:**
- Redis down → rate limiter fails open (Spring Cloud Gateway default behavior)
- Payment service down → circuit breaker opens → `FallbackController` returns 503
- JWT invalid/expired → 401 Unauthorized (no forwarding)

**Scaling Strategy:**
- Stateless — scale horizontally behind a load balancer
- Redis connection shared across instances for consistent rate limit counting
- No local state except the Netty event loop

---

### 3.2 Payment Service (Spring Boot 3.4.4 — :8080)

**Responsibilities:**
- REST API for payment creation and retrieval
- Orchestrates the entire payment lifecycle (PENDING → SUCCESS/FAILED/FRAUD_REJECTED)
- Outbox pattern implementation (DB → Kafka consistency)
- Kafka consumers for all event types
- Double-entry ledger with invariant enforcement
- Idempotency enforcement (Redis + DB)
- Rate limiting (Redis Lua + Caffeine fallback)
- Webhook delivery with exponential backoff
- Reconciliation (scheduled every 30 minutes)

**APIs:**

| Endpoint | Method | Headers | Request Body | Response | Status |
|---|---|---|---|---|---|
| `/payments` | POST | `Idempotency-Key` (required) | `{userId, merchantId, amount, currency, description}` | `PaymentResponse` | 201 Created |
| `/payments/{id}` | GET | — | — | `PaymentResponse` | 200 OK |
| `/webhooks` | POST | — | `{merchantId, url, events}` | `{id, url, events, secret, active}` | 201 Created |
| `/webhooks/{id}` | DELETE | — | — | — | 204 No Content |

**Internal Logic (createPayment):**
```
@Transactional(isolation = READ_COMMITTED)
1. RateLimiterService.checkRateLimit(userId)
   └─ Redis Lua: INCR rate_limit:<userId>; EXPIRE 60s
   └─ If Redis down: Caffeine in-memory (50K entries, 2min TTL)
   └─ Throws RateLimitExceededException → 429

2. PaymentCacheService.getPaymentIdForIdempotencyKey(key)
   └─ Redis GET idem:<key> → if found, return cached PaymentResponse (short circuit)

3. PaymentRepository.findByIdempotencyKeyForUpdate(key)
   └─ SELECT ... FOR UPDATE → pessimistic lock prevents concurrent race
   └─ If found, return existing payment (idempotent replay)

4. Save Payment(status=PENDING) + OutboxEvent(topic=payment.created) → SAME transaction
   └─ This is the critical atomicity guarantee — if either fails, both roll back

5. Cache: idem:<key> → paymentId (TTL 24h), payment:<id> → PaymentResponse (TTL 30min)

6. PaymentMetrics.recordPaymentCreated(currency)
7. Return PaymentResponse with 201
```

**Failure Scenarios:**
- DB connection timeout → HikariCP exhausted → requests fail with 500
- Redis down → rate limiter falls back to Caffeine; idempotency check falls through to DB; cache misses go to DB
- Kafka consumer crash → unacked messages redelivered; `processed_events` dedup prevents double-processing
- Concurrent idempotency race → `SELECT ... FOR UPDATE` serializes access; `UNIQUE` constraint is last-resort guard

**Scaling Strategy:**
- Stateless application — horizontally scalable
- HikariCP connection pool: min 5, max 20 connections per instance
- Kafka consumer concurrency: 3 threads per instance (configurable)
- Webhook delivery: dedicated thread pool (core=5, max=20, queue=500)
- Redis Lettuce pool: max-active=20, max-idle=10 per instance

---

### 3.3 Fraud Detection Service (FastAPI / Python 3.11 — :8000)

**Responsibilities:**
- Two-stage fraud scoring (rule engine + ML)
- Kafka consumer pipeline (`fraud.request` → `fraud.result`)
- HTTP API for synchronous scoring (`POST /score`)
- Velocity tracking (Redis, with in-process fallback)
- Prometheus metrics export

**APIs:**

| Endpoint | Method | Request | Response |
|---|---|---|---|
| `/score` | POST | `{payment_id, user_id, amount, currency, correlation_id}` | `{payment_id, fraud, reason, score, fallback, stage}` |
| `/health` | GET | — | `{status: "ok", model: "IsolationForest", version: "1.0.0"}` |
| `/metrics` | GET | — | Prometheus exposition format |

**Internal Logic:**
```
Stage 1 — Rule Engine (deterministic, <1ms):
  ├─ Currency not in allowlist (7 currencies) → REJECT (score=0.95, stage="rule")
  ├─ Amount > $10,000 → REJECT (score=0.95, stage="rule")
  └─ Velocity > 10 tx in 60s for same user → REJECT (score=0.95, stage="rule")

Stage 2 — ML Scoring (IsolationForest, ~1-5ms):
  ├─ Feature extraction: [amount, is_high_value (>$5K), currency_risk_score]
  ├─ IsolationForest.decision_function() → raw score [-0.5, 0.5]
  ├─ Normalize: fraud_probability = max(0, min(1, 0.5 - raw_score))
  └─ Score ≥ 0.75 → FRAUD (stage="combined")
      Score < 0.75 → PASS (stage="ml")
```

**Kafka Consumer Architecture:**
- `NUM_WORKERS` (default=3) daemon threads, each with its own `confluent_kafka.Consumer` instance
- confluent-kafka `Consumer` is NOT thread-safe — each thread gets a dedicated instance
- All workers join the same consumer group (`fraud-service-group`)
- Kafka rebalances partitions across all workers (in-process + cross-pod)
- Manual synchronous commit after processing each message
- Unhandled exceptions → 1s backoff → retry (infinite loop with per-message error isolation)

**Failure Scenarios:**
- Redis down → velocity check falls back to in-process `deque` per user (per-worker, not global — acceptable for fraud heuristics)
- ML model error → exception caught in consumer loop, 1s backoff, message retried
- Kafka broker unreachable → consumer poll returns None, loop continues

**Scaling Strategy:**
- Scale pods horizontally; each pod's `NUM_WORKERS` threads join the same consumer group
- Max parallelism = `min(total_workers_across_pods, num_partitions_on_fraud.request)`
- CPU-bound ML scoring: increase `NUM_WORKERS` to match available cores
- Hot-user mitigation: composite partition key `userId:seq%SALT` distributes across partitions

---

### 3.4 Reconciliation Service (embedded in Payment Service)

**Responsibilities:** Detect and alert on data integrity violations.

**Runs:** `@Scheduled(cron = "0 0/30 * * * *")` — every 30 minutes.

**5 Reconciliation Checks:**

| Check | What It Detects | Query |
|---|---|---|
| `STALE_PAYMENT` | Payments stuck in PENDING > 10 minutes | `findByStatusAndCreatedAtBefore(PENDING, now - threshold)` |
| `GHOST_PAYMENT` | Payments marked SUCCESS but no ledger entries | `status=SUCCESS AND NOT EXISTS ledger_entries` |
| `DUPLICATE_LEDGER` | Payments with > 2 ledger entries (should be exactly 2) | `GROUP BY payment_id HAVING COUNT > 2` |
| `LEDGER_IMBALANCE` | Payments where `SUM(DEBIT) != SUM(CREDIT)` | `HAVING SUM(DEBIT_amount) != SUM(CREDIT_amount)` |
| `ORPHAN_LEDGER` | Ledger entries with no matching payment record | `NOT EXISTS (SELECT p FROM Payment)` |

**Output:** Publishes alert events to Kafka topic `reconciliation.alerts`.

---

### 3.5 Notification Service (embedded in Payment Service)

**Responsibilities:** Webhook delivery to merchant endpoints.

**Webhook Delivery Flow:**
```
1. PaymentProcessedConsumer receives payment.processed / payment.failed
2. PaymentNotificationService.dispatchNotification() — @Transactional
   ├─ Dedup check via IdempotentConsumerService
   └─ WebhookService.dispatchEvent() — @Async("webhookExecutor")
       ├─ Find active endpoints: findActiveEndpointsForEvent(merchantId, eventType)
       ├─ Idempotency: existsByIdempotencyKey("<endpointId>:<paymentId>:<eventType>")
       ├─ Save WebhookDelivery(status=PENDING)
       └─ attemptDelivery():
           ├─ POST to endpoint URL with headers:
           │   X-Payment-Event, X-Delivery-Id, X-Idempotency-Key
           ├─ Timeout: 10s connect + 10s read
           ├─ 2xx → status=SUCCESS
           └─ Non-2xx/timeout → handleDeliveryFailure():
               ├─ Exponential backoff: [5s, 30s, 120s, 600s, 1800s]
               ├─ Set nextRetryAt and status=FAILED
               └─ After max_retry_attempts (5) → status=DLQ
3. Retry scheduler: @Scheduled(fixedDelay=30s)
   └─ Find FAILED deliveries where nextRetryAt < now → re-attempt
```

---

### 3.6 Frontend Dashboard (React / TypeScript / Vite)

**Components:**

| Component | Data Source |
|---|---|
| MetricsBar | REST API — aggregated payment counts |
| PaymentTable | REST + WebSocket real-time updates |
| CreatePaymentForm | POST /payments via REST |
| EventStream | WebSocket `/ws/payments` — Kafka event timeline |
| SystemStatus | Health endpoints + WebSocket connectivity |
| AlertPanel | Fraud rejections and failed payments |

**WebSocket Architecture:**
- Endpoint: `ws://host:8080/ws/payments`
- `PaymentWebSocketHandler` manages sessions via `WebSocketSessionManager` (ConcurrentHashMap)
- Broadcast is fire-and-forget, outside transaction boundaries
- Optional Redis pub/sub mode (`app.websocket.mode=redis`) for multi-instance broadcasting via `RedisWebSocketBroadcaster` → `RedisWebSocketSubscriber`

---

## 4. Data Modeling

### 4.1 PostgreSQL Schema

#### `payments` Table

```sql
CREATE TABLE payments (
    id                UUID         PRIMARY KEY,            -- NOT auto-generated, set by app
    user_id           UUID         NOT NULL,
    merchant_id       UUID         NOT NULL,
    amount            NUMERIC(19,4) NOT NULL,              -- precision=19, scale=4
    currency          VARCHAR(3)   NOT NULL,
    status            VARCHAR(255) NOT NULL,               -- ENUM: PENDING|PROCESSING|SUCCESS|FAILED|FRAUD_REJECTED|RECONCILED
    idempotency_key   VARCHAR(255) NOT NULL UNIQUE,        -- critical dedup constraint
    description       VARCHAR(512),
    failure_reason    VARCHAR(255),
    fraud_score       DOUBLE PRECISION,
    correlation_id    VARCHAR(255),
    trace_id          VARCHAR(255),
    created_at        TIMESTAMP    NOT NULL,               -- set via @PrePersist
    updated_at        TIMESTAMP
);

-- Indexes
CREATE UNIQUE INDEX uq_payments_idempotency_key ON payments(idempotency_key);
CREATE INDEX idx_payments_user_id    ON payments(user_id);
CREATE INDEX idx_payments_status     ON payments(status);
CREATE INDEX idx_payments_created_at ON payments(created_at);
```

**Design Decisions:**
- `NUMERIC(19,4)` for amount: precision=19 supports up to $999,999,999,999,999.9999 — sufficient for any real-world payment. Scale=4 handles sub-cent precision required for currency conversions and fee calculations.
- `idempotency_key UNIQUE`: DB-level last-resort guard against duplicate payment creation. The application checks Redis first (cheap), then DB (authoritative).
- UUID primary key: avoids auto-increment contention in distributed scenarios and prevents ID enumeration attacks.
- `status` as VARCHAR (not enum): allows adding new statuses without schema migration. The enum is enforced at the Java application layer (`@Enumerated(STRING)`).
- Separate `created_at` / `updated_at`: immutable creation timestamp for event ordering; mutable update for status tracking.
- `trace_id` and `correlation_id` stored on the row: enables end-to-end request tracing without joining log aggregation systems.

#### `ledger_entries` Table

```sql
CREATE TABLE ledger_entries (
    id                 UUID           PRIMARY KEY,
    payment_id         UUID           NOT NULL,            -- FK conceptual (no DB FK for perf)
    entry_type         VARCHAR(255)   NOT NULL,            -- 'DEBIT' or 'CREDIT'
    amount             NUMERIC(19,4)  NOT NULL,
    currency           VARCHAR(3)     NOT NULL,
    account_id         VARCHAR(255)   NOT NULL,            -- e.g., "merchant:<merchantId>"
    counter_account_id VARCHAR(255)   NOT NULL,            -- e.g., "user:<userId>"
    correlation_id     VARCHAR(255),
    created_at         TIMESTAMP      NOT NULL
);

-- Indexes
CREATE INDEX idx_ledger_payment_id  ON ledger_entries(payment_id);
CREATE INDEX idx_ledger_account_id  ON ledger_entries(account_id);
CREATE INDEX idx_ledger_created_at  ON ledger_entries(created_at);
```

**Design Decisions:**
- **Double-entry bookkeeping**: Every payment generates exactly 2 entries — a DEBIT from the payment source and a CREDIT to the merchant. The invariant `SUM(DEBIT) == SUM(CREDIT)` for any payment is enforced programmatically in `LedgerService.validateInvariant()`.
- No foreign key to `payments`: deliberate choice. FK constraints add overhead to every INSERT and prevent flexible cleanup/archival. Integrity is enforced at the application layer and verified by reconciliation.
- `account_id` as VARCHAR: supports arbitrary account naming schemes (merchant accounts, system accounts, fee accounts) without needing an `accounts` table. Enables easy extension to multi-account scenarios (escrow, platform fees).
- `COALESCE(SUM(amount), 0)` queries for invariant checks: handles the edge case where one side has entries but the other doesn't yet.

#### `accounts` Table

```sql
CREATE TABLE accounts (
    id              UUID           PRIMARY KEY,
    owner_id        VARCHAR(255)   NOT NULL,              -- userId or merchantId
    account_type    VARCHAR(255)   NOT NULL,              -- 'USER' or 'MERCHANT'
    balance         NUMERIC(19,4)  NOT NULL DEFAULT 0,
    currency        VARCHAR(3)     NOT NULL,
    version         BIGINT         NOT NULL DEFAULT 0,    -- JPA @Version for optimistic locking
    created_at      TIMESTAMP      NOT NULL,
    updated_at      TIMESTAMP
);

-- Unique constraint: one account per owner per currency
ALTER TABLE accounts ADD CONSTRAINT uk_account_owner_currency UNIQUE (owner_id, currency);
CREATE INDEX idx_account_owner_id ON accounts(owner_id);
CREATE INDEX idx_account_type     ON accounts(account_type);
```

**Design Decisions:**
- **Optimistic locking via `@Version`**: JPA increments `version` on every UPDATE. If two transactions read the same version and both try to update, the second one gets `OptimisticLockException` — the caller retries.
- **Lazy provisioning**: accounts are auto-created on first payment (user accounts with $100K default balance, merchant accounts with $0). Production systems would have a separate onboarding flow.
- **Balance constraint**: user accounts enforce non-negative balance (checked in `AccountService.debit()`). Merchant accounts allow any value (credit side).
- **One account per owner per currency**: the `UNIQUE(owner_id, currency)` constraint supports multi-currency without separate tables.

#### `outbox_events` Table

```sql
CREATE TABLE outbox_events (
    id              UUID           PRIMARY KEY,
    event_type      VARCHAR(255)   NOT NULL,              -- Kafka topic name
    aggregate_id    VARCHAR(255)   NOT NULL,              -- paymentId
    payload         TEXT           NOT NULL,              -- JSON serialized event
    status          VARCHAR(255)   NOT NULL DEFAULT 'NEW', -- NEW|PUBLISHED|FAILED
    retry_count     INTEGER        NOT NULL DEFAULT 0,
    partition_key   VARCHAR(255),                         -- userId (for Kafka partitioning)
    correlation_id  VARCHAR(255),
    trace_id        VARCHAR(255),                         -- stored explicitly, no JSON parsing
    created_at      TIMESTAMP      NOT NULL,
    published_at    TIMESTAMP,
    last_error      TEXT
);

-- Indexes
CREATE INDEX idx_outbox_status     ON outbox_events(status);
CREATE INDEX idx_outbox_created_at ON outbox_events(created_at);
```

**Design Decisions:**
- `partition_key` stored separately: the outbox publisher uses this as the Kafka message key. Storing it avoids JSON parsing during the hot polling loop.
- `trace_id` stored as explicit column: enables correlation without payload deserialization. The publisher reads it directly and sets it as a Kafka header.
- `last_error` as TEXT: captures full stack traces for debugging failed publishes.
- `retry_count` + `max_retries` config (default 3): after exhausting retries, status → FAILED. A separate cleanup job can delete or alert on FAILED events.
- Batch size 50 per poll: balances throughput (fewer polls) vs transaction duration (don't hold DB lock too long).

#### `processed_events` Table

```sql
CREATE TABLE processed_events (
    event_id       VARCHAR(512)   PRIMARY KEY,            -- composite: "<topic>:<partitionKey>:<eventId>"
    consumer_group VARCHAR(255)   NOT NULL,
    topic          VARCHAR(255)   NOT NULL,
    processed_at   TIMESTAMP      NOT NULL
);
```

**Design Decisions:**
- **The deduplication table**: this is the core of exactly-once processing. When a Kafka consumer processes a message, it first attempts to INSERT into this table inside the same transaction. If a `DataIntegrityViolationException` is thrown (PK violation), the message was already processed — skip it.
- Composite PK format `<topic>:<partitionKey>:<eventId>`: ensures uniqueness across topics and consumer groups. Two different consumers processing the same event get different composite keys.
- Retention: weekly cleanup (`@Scheduled cron "0 0 2 * * SUN"`) deletes entries older than 7 days. This bounds table growth while covering Kafka's 7-day retention window.

#### `webhook_endpoints` Table

```sql
CREATE TABLE webhook_endpoints (
    id          UUID         PRIMARY KEY,
    merchant_id UUID         NOT NULL,
    url         VARCHAR(255) NOT NULL,
    secret      VARCHAR(255) NOT NULL,                    -- HMAC-SHA256 signing secret
    events      VARCHAR(255) NOT NULL,                    -- "ALL" or "payment.processed,payment.failed"
    active      BOOLEAN      NOT NULL DEFAULT true,
    created_at  TIMESTAMP    NOT NULL
);

CREATE INDEX idx_webhook_merchant_id ON webhook_endpoints(merchant_id);
```

#### `webhook_deliveries` Table

```sql
CREATE TABLE webhook_deliveries (
    id                   UUID         PRIMARY KEY,
    webhook_endpoint_id  UUID         NOT NULL,
    payment_id           UUID         NOT NULL,
    event_type           VARCHAR(255) NOT NULL,
    payload              TEXT         NOT NULL,
    idempotency_key      VARCHAR(255) NOT NULL UNIQUE,    -- "<endpointId>:<paymentId>:<eventType>"
    status               VARCHAR(255) NOT NULL DEFAULT 'PENDING',  -- PENDING|SUCCESS|FAILED|DLQ
    attempt_count        INTEGER      NOT NULL DEFAULT 0,
    last_response_code   INTEGER,
    last_error           VARCHAR(255),
    next_retry_at        TIMESTAMP,
    created_at           TIMESTAMP    NOT NULL,
    last_attempted_at    TIMESTAMP
);

CREATE INDEX idx_wh_delivery_status     ON webhook_deliveries(status);
CREATE INDEX idx_wh_delivery_payment_id ON webhook_deliveries(payment_id);
CREATE INDEX idx_wh_delivery_next_retry ON webhook_deliveries(next_retry_at);
```

### 4.2 Index Strategy

| Index | Purpose | Query Pattern |
|---|---|---|
| `uq_payments_idempotency_key` | Unique constraint + fast lookup | `findByIdempotencyKey(key)`, `SELECT ... FOR UPDATE` |
| `idx_payments_user_id` | User-scoped queries | Rate limit counting, user payment history |
| `idx_payments_status` | Reconciliation + monitoring | `findByStatusAndCreatedAtBefore(PENDING, ...)` |
| `idx_payments_created_at` | Time-range queries | Reconciliation stale payment detection |
| `idx_ledger_payment_id` | Payment-to-ledger join | `findByPaymentId`, `existsByPaymentId`, invariant queries |
| `idx_outbox_status` | Polling hot path | `findTop50ByStatusOrderByCreatedAtAsc(NEW)` — this runs every 1s |
| `idx_wh_delivery_next_retry` | Retry scheduler | `findByStatusAndNextRetryAtBefore(FAILED, now)` |

### 4.3 Idempotency Enforcement — Multi-Layer

```
Layer 1 (Fastest — Redis): GET idem:<key>
  └─ Cache hit → return cached PaymentResponse (no DB hit)

Layer 2 (Authoritative — DB pessimistic lock): SELECT ... FOR UPDATE WHERE idempotency_key = ?
  └─ Row exists → return existing payment
  └─ Row doesn't exist → proceed to create

Layer 3 (Last resort — DB UNIQUE constraint): INSERT INTO payments (..., idempotency_key, ...)
  └─ DataIntegrityViolationException → caught by GlobalExceptionHandler → 409 Conflict
```

This three-layer approach ensures:
1. Most idempotent replays are served from Redis (sub-ms, no DB load)
2. Concurrent requests for the same key are serialized by `FOR UPDATE`
3. Any remaining race condition is caught by the `UNIQUE` constraint

---

## 5. Kafka / Event Streaming Design

### 5.1 Topics and Purpose

| Topic | Partitions | Purpose | Producer | Consumer |
|---|---|---|---|---|
| `payment.created` | 3 | New payment submitted | OutboxPublisherService | PaymentEventConsumer |
| `fraud.request` | 3 | Payment forwarded for fraud check | PaymentEventConsumer | Fraud Service (3 workers) |
| `fraud.result` | 3 | Fraud scoring result | Fraud Service | FraudResultConsumer |
| `payment.processed` | 3 | Payment succeeded | OutboxPublisherService | PaymentProcessedConsumer |
| `payment.failed` | 3 | Payment failed / fraud rejected | OutboxPublisherService | PaymentProcessedConsumer |
| `fraud.alerts` | 3 | Fraud detection alerts | OutboxPublisherService | (external monitoring) |
| `reconciliation.alerts` | 3 | Reconciliation anomalies | ReconciliationService | (external monitoring) |

**DLQ Topics (1 partition each):**

| Topic | Source |
|---|---|
| `payment.created.DLQ` | PaymentEventConsumer unrecoverable errors |
| `payment.processed.DLQ` | PaymentProcessedConsumer unrecoverable errors |
| `fraud.request.DLQ` | (reserved) |
| `fraud.result.DLQ` | FraudResultConsumer unrecoverable errors |

### 5.2 Partitioning Strategy

**Partition key: `userId`** (set as `partitionKey` in `OutboxEvent`, used in `OutboxPublisherService`).

Why `userId` instead of `paymentId`:
- **Per-user ordering guarantee**: all events for a single user land on the same partition → processed in order. This prevents race conditions like processing a user's second payment before the first.
- **Fraud velocity tracking**: the fraud service's per-user velocity counter works correctly because all events for a user go to the same partition (and therefore the same consumer thread in a single-threaded-per-partition model).

Trade-off — hot user problem:
- A high-volume user's events all go to one partition, creating a hot partition
- Mitigation: composite key `userId + ":" + (seq % SALT_FACTOR)` distributes across SALT_FACTOR partitions at the cost of per-user ordering (acceptable for fraud scoring, as noted in the fraud service code)

When `partitionKey` is null, falls back to `aggregateId` (paymentId) — giving uniform distribution.

### 5.3 Message Schema Design

**Payment Event (v1):**
```json
{
  "version": "v1",
  "eventId": "uuid",
  "traceId": "uuid",
  "paymentId": "uuid",
  "userId": "uuid",
  "merchantId": "uuid",
  "amount": 99.99,
  "currency": "USD",
  "status": "PENDING",
  "idempotencyKey": "client-key-123",
  "description": "Widget purchase",
  "correlationId": "uuid",
  "eventType": "payment.created",
  "createdAt": "2026-03-28T10:00:00",
  "eventTimestamp": "2026-03-28T10:00:00.123"
}
```

**Kafka Headers (all messages):**

| Header | Purpose |
|---|---|
| `x-event-id` | Deduplication key; uniquely identifies this event |
| `x-correlation-id` | Traces a single user request across all services |
| `x-trace-id` | Links to the original HTTP request for distributed tracing |

Headers are propagated through the entire pipeline: payment-service → fraud.request → fraud-service → fraud.result → back to payment-service. The `KafkaHeaderUtil` utility class handles encoding/decoding.

### 5.4 Consumer Group Strategy

| Consumer Group | Members | Topics | Purpose |
|---|---|---|---|
| `payment-processor-group` | PaymentEventConsumer, FraudResultConsumer | `payment.created`, `fraud.result` | Core payment lifecycle processing |
| `payment-notification-group` | PaymentProcessedConsumer | `payment.processed`, `payment.failed` | Notification/webhook dispatch |
| `fraud-service-group` | Fraud Service workers (3 threads × N pods) | `fraud.request` | ML scoring |

Separating notification into its own group (`payment-notification-group`) ensures:
1. Notification failures don't block payment state transitions
2. Notification consumers can be scaled independently
3. Replay of notifications doesn't re-trigger payment processing

### 5.5 Delivery Semantics

**Producer:** Effectively exactly-once.
- `acks=all`: all in-sync replicas must acknowledge
- `enable.idempotence=true`: Kafka assigns sequence numbers to prevent duplicate writes from producer retries
- `max.in.flight.requests.per.connection=1`: prevents reordering during retries
- `retries=5`, `delivery.timeout.ms=120000`: 2-minute window for transient failures

**Consumer:** At-least-once delivery + application-level deduplication = effectively exactly-once processing.
- `enable.auto.commit=false`, `AckMode=MANUAL_IMMEDIATE`: consumer explicitly acks after processing
- Deduplication via `processed_events` table (PK constraint within the same transaction as business logic)
- If a consumer crashes before acking, Kafka redelivers the message; the dedup table catches the duplicate

**Why not Kafka transactions (exactly-once)?**
- Kafka transactions (EOS) tie the consumer and producer into a single transactional unit, but our business logic involves PostgreSQL writes that are outside Kafka's transaction coordinator
- The outbox + dedup pattern achieves the same end-to-end guarantee while keeping Kafka and PostgreSQL transaction boundaries independent
- Kafka EOS also has performance overhead (~30% throughput reduction) and operational complexity

### 5.6 Duplicate Handling

Duplicates can arise from:

| Source | Cause | Mitigation |
|---|---|---|
| Producer retries | Network timeout after broker committed | `enable.idempotence=true` — Kafka deduplicates at the broker level |
| Consumer crash before ack | Consumer processes message, crashes before `ack()` | `processed_events` table — PK insert fails on redelivery |
| Outbox poller retry | Outbox event published to Kafka but DB update to PUBLISHED failed | Kafka idempotent producer prevents duplicate message; consumers dedup via `processed_events` |
| Webhook delivery | Endpoint timeout after receiving payload | `webhook_deliveries.idempotency_key` UNIQUE constraint prevents duplicate delivery records |

---

## 6. Distributed Systems Patterns

### 6.1 Idempotency

**Where:** Payment creation API, every Kafka consumer, webhook delivery.

**Payment API Idempotency — Implementation:**

```
Client sends: POST /payments + Idempotency-Key: "abc-123"

PaymentService.createPayment():
  1. Redis lookup: GET idem:abc-123
     ├─ HIT: return cached PaymentResponse (zero DB cost)
     └─ MISS: continue

  2. DB pessimistic lock: SELECT * FROM payments WHERE idempotency_key='abc-123' FOR UPDATE
     ├─ ROW EXISTS: return existing payment (concurrent request lost the race)
     └─ NO ROW: continue (we hold the lock, no other TX can insert this key)

  3. INSERT Payment + OutboxEvent in same TX
     ├─ SUCCESS: cache in Redis, return 201
     └─ DataIntegrityViolationException: another request beat us despite FOR UPDATE
         (possible only on connection pool exhaustion causing lock timeout)
         → GlobalExceptionHandler → 409 Conflict
```

The `FOR UPDATE` lock is critical: without it, two concurrent requests for the same idempotency key could both pass the Redis check (miss) and the `findByIdempotencyKey` check (not yet committed), then both attempt to INSERT, and one would fail with a constraint violation. With `FOR UPDATE`, the second request blocks until the first commits.

**Kafka Consumer Idempotency — Implementation:**

```
IdempotentConsumerService.tryMarkProcessed(eventId, consumerGroup, topic):
  INSERT INTO processed_events (event_id, consumer_group, topic, processed_at)
  VALUES ('<topic>:<partitionKey>:<eventId>', ?, ?, now())

  ├─ SUCCESS: return true → proceed with business logic
  └─ DataIntegrityViolationException (PK violation): return false → skip (already processed)
```

This runs inside `@Transactional(propagation=MANDATORY)` — it MUST be called within an existing transaction. If the business logic transaction rolls back, the dedup insert also rolls back, so the message will be reprocessed on the next delivery — which is correct.

### 6.2 Transactional Outbox Pattern

**Problem:** Writing to PostgreSQL and publishing to Kafka must appear atomic. If we write to DB then publish to Kafka, a crash between the two operations means the event is lost. If we publish to Kafka then write to DB, a DB failure means we sent an event for a nonexistent payment.

**Solution:**

```
Step 1 — Write Phase (single DB transaction):
  BEGIN TX
    INSERT INTO payments (...)
    INSERT INTO outbox_events (event_type='payment.created', payload=JSON, status='NEW')
  COMMIT
  // Both succeed or both fail. Kafka is NOT involved here.

Step 2 — Publish Phase (OutboxPublisherService, polled every 1s):
  BEGIN TX
    SELECT TOP 50 FROM outbox_events WHERE status='NEW' ORDER BY created_at ASC
    FOR EACH event:
      kafkaTemplate.send(ProducerRecord(topic, key, value, headers))
        .addCallback(
          onSuccess: UPDATE outbox_events SET status='PUBLISHED', published_at=now()
          onFailure: UPDATE outbox_events SET retry_count++, last_error=err.message
        )
      IF retry_count >= max_retries:
        UPDATE outbox_events SET status='FAILED'
  COMMIT
```

**Why this works:**
- The payment and the outbox event are in the same DB transaction — they can never diverge
- The publisher is a separate scheduled process that retries until the event is published
- If the publisher crashes, the event stays in `NEW` state and is picked up on restart
- If Kafka accepts the message but the DB update to `PUBLISHED` fails, the publisher will resend — Kafka's idempotent producer deduplicates at the broker level

**Latency impact:** The outbox adds ~1s median delay (polling interval) between payment creation and the first Kafka event. This is acceptable because the client receives a synchronous 201 response immediately — the async pipeline is for downstream processing.

### 6.3 Saga Pattern (Payment Lifecycle)

The payment lifecycle is an implicit saga with the following states:

```
PENDING ──────────────────────────────────────────┐
    │                                              │
    │ OutboxPublisher → payment.created            │
    ▼                                              │
PaymentEventConsumer                               │
    │                                              │
    │ → fraud.request                              │
    ▼                                              │ Timeout (ReconciliationService
Fraud Service                                      │ detects STALE_PAYMENT after 10min)
    │                                              │
    │ → fraud.result                               │
    ▼                                              │
FraudResultConsumer                                │
    │                                              ▼
    ├── fraud=true  → FRAUD_REJECTED ─────► payment.failed
    │                                       fraud.alerts
    └── fraud=false → Ledger (DEBIT+CREDIT)
                      ├── Invariant OK → SUCCESS ──► payment.processed
                      └── Invariant FAIL → rolls back TX → retried 3x → DLQ
```

**Compensation (when things go wrong):**
- If fraud check never returns: `ReconciliationService` detects `STALE_PAYMENT` after 10 minutes, publishes `reconciliation.alerts`
- If ledger invariant fails: the entire `processPaymentResult()` transaction rolls back; `@Retryable` retries 3 times with exponential backoff; after exhaustion, `@Recover` re-throws, sends to DLQ
- If SUCCESS payment has no ledger entries (shouldn't happen): `GHOST_PAYMENT` reconciliation check catches it

### 6.4 Retry Strategy

| Component | Strategy | Config |
|---|---|---|
| `processPaymentResult()` | `@Retryable`: 3 attempts, 200ms initial, 2× multiplier, max 2s | `TransientDataAccessException` only |
| Kafka consumer container | `ExponentialBackOff`: 1000ms initial, 2.0 multiplier, max 4 attempts | All exceptions |
| Outbox publisher | Retry counter per event, max 3 retries | On Kafka send failure |
| Webhook delivery | Exponential backoff: [5s, 30s, 120s, 600s, 1800s] | On non-2xx response |
| Kafka producer | 5 retries, 120s delivery timeout | Transient broker failures |
| API Gateway (/api) | 3 retries on 502/503/504, backoff 100ms→500ms, GET only | Downstream service restart |

### 6.5 Dead Letter Queue (DLQ)

Every Kafka consumer follows the same DLQ pattern:

```java
try {
    // Business logic
} catch (Exception e) {
    log.error("Unrecoverable error processing message", e);
    kafkaTemplate.send("<original-topic>.DLQ", record.key(), record.value());
} finally {
    acknowledgment.acknowledge();   // Always ack to prevent infinite redelivery
}
```

**Why always ack?** Without acknowledging, a poison message would block the partition indefinitely. By sending to DLQ and acking, the consumer moves forward. DLQ messages can be investigated and replayed manually.

**DLQ topics are single-partition** (1 partition, 1 replica) because they are low-volume error queues, not high-throughput paths.

### 6.6 Circuit Breaker

**Where:** `FraudServiceClient.check()` — the synchronous HTTP fallback path to the fraud service.

**Implementation:** Resilience4j `@CircuitBreaker`

```
Configuration (application.yml):
  slidingWindowType: COUNT_BASED
  slidingWindowSize: 10
  minimumNumberOfCalls: 5
  failureRateThreshold: 50%        — opens after 5/10 calls fail
  waitDurationInOpenState: 10s     — stays open for 10s before half-open
  permittedNumberOfCallsInHalfOpenState: 3
  automaticTransitionFromOpenToHalfOpenEnabled: true

Fallback (when circuit is OPEN):
  Returns: fraud=false, score=0.5, reason="fraud_service_unavailable", fallback=true
```

**This is a fail-OPEN design:** when the fraud service is down, payments are ALLOWED through (not blocked). This is a deliberate business decision:
- Blocking all payments during a fraud service outage causes revenue loss
- The `fallback=true` flag is recorded, enabling retroactive review
- The fraud score of 0.5 is neutral — not clean, not flagged
- Alternative: fail-CLOSED (block all payments) — appropriate for high-risk merchants

**API Gateway also uses circuit breakers:** Spring Cloud Gateway's `CircuitBreaker` filter opens when payment-service returns persistent errors, forwarding to `FallbackController` which returns 503.

---

## 7. Fraud Detection System

### 7.1 Architecture

```
                    ┌──────────────────────────────────────┐
                    │           Fraud Service               │
                    │                                      │
  fraud.request ──▶ │  ┌─────────────┐   ┌──────────────┐  │ ──▶ fraud.result
                    │  │ Rule Engine  │──▶│ ML Scoring    │  │
                    │  │ (Stage 1)    │   │ (Stage 2)     │  │
                    │  └─────────────┘   └──────────────┘  │
                    │         │                  │          │
                    │    Deterministic      Probabilistic   │
                    │     <1ms latency      ~1-5ms latency │
                    └──────────────────────────────────────┘
```

### 7.2 Rule Engine (Stage 1)

| Rule | Threshold | Rationale |
|---|---|---|
| **Currency Allowlist** | 7 currencies (USD, EUR, GBP, INR, JPY, CAD, AUD) | Unlisted currencies carry high fraud risk due to limited monitoring |
| **Amount Threshold** | > $10,000 | Large transactions require manual review in many jurisdictions |
| **Velocity Check** | > 10 transactions per user in 60 seconds | Card testing attacks typically involve rapid-fire small transactions |

**Velocity implementation:**
- Primary: Redis `INCR` + `EXPIRE` on key `velocity:<userId>` (atomic, shared across workers)
- Fallback: In-process `collections.deque` per user with timestamp-based window (per-worker, not global)
- Trade-off: Redis gives global velocity counting; in-process fallback gives per-worker counting (may under-count if events are spread across workers)

**Stage 1 is a short-circuit:** if any rule fires, the transaction is immediately rejected (score=0.95, stage="rule"). ML scoring is skipped entirely.

### 7.3 ML Model Integration

**Model:** scikit-learn `IsolationForest`
- Contamination: 0.1 (10% of training data is anomalous)
- Estimators: 100 trees
- Trained at startup on synthetic data (in production: loaded from S3/GCS)

**Feature Extraction:**

| Feature | Derivation | Risk Signal |
|---|---|---|
| `amount` | Raw transaction amount | Log-normal distribution in legitimate transactions; extreme values indicate fraud |
| `is_high_value` | `1.0 if amount > 5000 else 0.0` | Binary flag amplifies the amount signal for the tree splits |
| `currency_risk` | Lookup table (USD=0.05, unlisted=0.9) | Proxy for jurisdictional fraud rates |

**Training Data (synthetic):**
- 900 normal: `lognormal(mean=4.0, sigma=1.5)` → ~$55 median, currency_risk uniform [0.0, 0.2]
- 100 fraudulent: `uniform(9000, 50000)`, currency_risk uniform [0.7, 1.0]

**Inference Flow:**
```python
raw_score = model.decision_function(features)[0]   # range: ~[-0.5, 0.5]
fraud_probability = max(0.0, min(1.0, 0.5 - raw_score))  # more negative = more fraud
# Threshold: score >= 0.75 → FRAUD
```

**Score Distribution:**

| Score Range | Interpretation |
|---|---|
| 0.00 - 0.30 | Low risk — clearly normal transaction |
| 0.30 - 0.50 | Medium risk — within normal distribution |
| 0.50 - 0.75 | Elevated risk — unusual but not conclusive |
| 0.75 - 1.00 | High risk — classified as fraud |

### 7.4 Latency vs Accuracy Trade-offs

| Decision | Latency Impact | Accuracy Impact |
|---|---|---|
| Rule engine short-circuits ML | Saves ~1-5ms for obvious fraud | May reject borderline cases that ML would pass |
| IsolationForest (not deep learning) | ~1-5ms inference | Lower accuracy than neural networks; sufficient for feature set size |
| 3 features only | Faster feature extraction | Misses behavioral signals (device, location, time-of-day) |
| Score threshold 0.75 | N/A (post-inference) | Higher threshold → fewer false positives, more false negatives |
| Kafka async pipeline | +1s (outbox) + poll latency | Decoupled from API latency; client doesn't wait for scoring |

### 7.5 Fallback Behavior

**If the fraud service is down:**

| Path | Behavior |
|---|---|
| **Kafka pipeline** (primary) | Messages queue in `fraud.request` topic. When fraud service recovers, consumer poll loop resumes processing. No data loss — Kafka retains messages for 7 days. |
| **HTTP fallback** (circuit breaker) | `FraudServiceClient.fallbackCheck()` returns `fraud=false, score=0.5, fallback=true`. Payment proceeds with a neutral score. The `fallback=true` flag enables retroactive review. |

---

## 8. Consistency Model

### 8.1 Consistency Spectrum

```
Strong ◄──────────────────────────────────────────────────────► Eventual

Ledger      Payment       Idempotency    Payment Status    Webhook     WebSocket
Entries     Creation      Key (DB)       (after fraud)     Delivery    Broadcast
│           │             │              │                 │           │
│ @TX       │ @TX         │ FOR UPDATE   │ @TX + outbox    │ @Async    │ fire-and-
│ invariant │ + outbox    │ + UNIQUE     │ + consumer      │ retry     │ forget
│ enforced  │ atomic      │              │ dedup           │ eventual  │
▼           ▼             ▼              ▼                 ▼           ▼
```

### 8.2 Strongly Consistent Components

**Ledger Entries:**
- Written inside `@Transactional` together with the payment status update
- After writing DEBIT + CREDIT, `validateInvariant()` verifies `SUM(DEBIT) == SUM(CREDIT)` for the payment
- Any mismatch throws `IllegalStateException` → transaction rolls back → retried up to 3 times → DLQ
- This is the most critical consistency guarantee in the system. A ledger imbalance means money was created or destroyed.

**Payment Creation + Outbox Event:**
- Single `@Transactional(isolation=READ_COMMITTED)` wraps both the payment INSERT and the outbox event INSERT
- If either fails, both roll back. The payment never exists without a corresponding outbox event.

**Idempotency Key:**
- `SELECT ... FOR UPDATE` provides serialization guarantees for concurrent requests with the same key
- The `UNIQUE` constraint is a DB-level invariant that cannot be violated regardless of application bugs

### 8.3 Eventually Consistent Components

**Payment Status:**
- The payment is created as PENDING (strongly consistent)
- The transition to SUCCESS/FAILED/FRAUD_REJECTED happens asynchronously via Kafka consumers
- Between creation and status update, there's a window where the status is stale (typically 1-3 seconds)
- This is acceptable because the client receives the PENDING status immediately and can poll or wait for a webhook

**Redis Cache:**
- The `payment:<id>` cache (TTL 30min) may serve stale data after a status update
- Mitigated by cache eviction in `processPaymentResult()` — after updating the DB status, the cache key is deleted
- Window of inconsistency: between the DB update and the cache eviction (~microseconds, same transaction boundary)

**Webhook Delivery:**
- Eventually consistent by design — merchant endpoints may be down for hours
- Retry schedule: [5s, 30s, 120s, 600s, 1800s] — up to ~43 minutes of retry window
- After 5 attempts → DLQ status (manual intervention required)

### 8.4 Why the Ledger MUST Be Strongly Consistent

In a payment system, the ledger is the system of record. Every dollar debited must equal every dollar credited. If this invariant breaks:
- Merchants may receive money that was never charged (revenue leak)
- Users may be charged without the merchant receiving funds (trust violation)
- Auditors/regulators require provable balance at any point in time

The system enforces this via:
1. **Application-level invariant check** (`LedgerService.validateInvariant()`) — runs inside the same transaction that writes ledger entries
2. **Reconciliation service** — catches any invariant violations that somehow slip through (defense in depth)
3. **No FK-based cascading deletes** — ledger entries can never be accidentally deleted by a parent record deletion

---

## 9. Failure Handling

### 9.1 Kafka Consumer Crash

**Scenario:** A consumer thread dies (OOM, unhandled exception, pod eviction) after processing a message but before acknowledging it.

**Recovery:**
1. Kafka detects the consumer is dead (session timeout: 30s, heartbeat interval: 10s)
2. Consumer group rebalances — the dead consumer's partitions are assigned to surviving consumers
3. The unacked message offset is replayed from the last committed offset
4. `IdempotentConsumerService.tryMarkProcessed()` recognizes the duplicate:
   - If the message was fully processed (TX committed): PK insert fails → message skipped
   - If the message was partially processed (TX rolled back): PK insert succeeds → message reprocessed from scratch

**Key design point:** The dedup insert and business logic are in the SAME transaction. If the business logic fails and rolls back, the dedup record also rolls back — so the message WILL be reprocessed on redelivery. This is correct behavior.

### 9.2 Duplicate Events

**Scenario:** Kafka delivers the same message twice due to producer retry, consumer rebalance, or network partition.

**Recovery:**
```
FraudResultConsumer receives fraud.result (eventId=X) for the second time:

1. processPaymentResult() starts a new @Transactional
2. Inside TX: IdempotentConsumerService.tryMarkProcessed("fraud.result:partitionKey:X", ...)
   └─ INSERT INTO processed_events → DataIntegrityViolationException (PK exists)
   └─ Returns false → processPaymentResult() returns early (no-op)
3. TX commits (empty — no writes)
4. Consumer acks
```

No double-charging, no double-ledger entries, no double-webhooks. Every consumer in the system follows this pattern.

### 9.3 Database Transaction Failure

**Scenario:** PostgreSQL is temporarily unreachable or returns a transient error (deadlock, connection timeout).

**Recovery:**

| Component | Behavior |
|---|---|
| `createPayment()` | HikariCP waits up to 30s for a connection; if exhausted, throws → 500 response. Client retries with same idempotency key. |
| `processPaymentResult()` | `@Retryable(TransientDataAccessException, maxAttempts=3, backoff=200ms×2)`. After 3 failures: `@Recover` re-throws → message sent to DLQ. |
| `OutboxPublisherService` | Transaction failure → outbox event stays in `NEW` state → retried on next poll (1s later). |
| `LedgerService.recordPayment()` | Runs inside the caller's transaction. If the invariant check fails, the entire TX rolls back including the dedup record → message will be retried. |

### 9.4 Fraud Service Timeout

**Scenario:** The fraud service takes longer than expected or is completely unreachable.

**Recovery (Kafka path — primary):**
- Messages accumulate in the `fraud.request` topic (retained for 7 days)
- When the fraud service recovers, its consumer workers drain the backlog
- The backlog creates a burst of `fraud.result` messages — handled by the concurrency-3 `FraudResultConsumer`
- During the outage, payments remain in PENDING state
- `ReconciliationService` detects STALE_PAYMENT after 10 minutes and publishes alerts

**Recovery (HTTP path — circuit breaker):**
- After 5/10 calls fail: circuit opens
- All subsequent calls route to `fallbackCheck()` — returns `fraud=false, fallback=true`
- After 10s: circuit transitions to half-open, allows 3 probe calls
- If probes succeed: circuit closes, normal operation resumes

### 9.5 Partial System Failures

**Redis down:**

| Feature | Fallback |
|---|---|
| Rate limiting | Caffeine in-memory cache (50K entries, 2min expiry) — rate limits are per-instance, not global |
| Idempotency cache | Redis miss → DB lookup (slightly slower, fully correct) |
| Payment cache | Redis miss → DB lookup (same correctness) |
| Fraud velocity | In-process `deque` per user (per-worker, may under-count) |
| WebSocket pub/sub | Falls back to direct `WebSocketSessionManager.broadcast()` (single-instance only) |

**Kafka broker down:**
- Outbox events accumulate in `outbox_events` table (status=NEW, retry_count incremented)
- After `max_retries` (3): events marked FAILED (manual intervention required)
- API continues to accept payments (synchronous path unaffected)
- Payment status updates stall (PENDING state persists until Kafka recovers)

**PostgreSQL down:**
- All writes fail immediately (no cache-based workaround for the write path)
- Read requests served from Redis cache until TTL expires
- This is the single most critical dependency — the system cannot function without the database

---

## 10. Performance & Scaling

### 10.1 Bottleneck Analysis

| Component | Bottleneck | Current Limit | Mitigation |
|---|---|---|---|
| **PostgreSQL** | Connection pool (20), write throughput | ~2,000 TPS (single instance) | HikariCP tuning, batch inserts (`batch_size=50`, `order_inserts=true`), read replicas |
| **Outbox Polling** | 50 events per poll × 1s interval = 50 events/s max | 50 events/s | Increase batch size, decrease interval, or add dedicated outbox worker |
| **Kafka** | 3 partitions per topic | 3 concurrent consumers per topic | Increase partition count for higher parallelism |
| **Fraud ML** | CPU-bound IsolationForest inference | ~5,000 inferences/s per core | Scale fraud service pods; increase `NUM_WORKERS` |
| **Redis** | Single-threaded command processing | ~100K ops/s | Redis Cluster for sharding; already using Lettuce connection pool |

### 10.2 Horizontal Scaling Strategy

```
                    Load Balancer
                    /     |     \
            API GW-1  API GW-2  API GW-3        ← Stateless, share Redis for rate limits
                    \     |     /
                    Load Balancer
                    /     |     \
         PaySvc-1   PaySvc-2   PaySvc-3          ← Each has own HikariCP pool to shared PG
             │          │          │
             └──────────┼──────────┘
                        │
              ┌─────────┼─────────┐
          Kafka     PostgreSQL   Redis
          (3+ brokers) (primary+  (cluster or
                       replicas)  sentinel)
                        │
              ┌─────────┼─────────┐
        FraudSvc-1  FraudSvc-2  FraudSvc-3       ← Same consumer group, Kafka rebalances
```

**Per-tier scaling:**
- **API Gateway:** Add instances behind load balancer. Redis rate limits are naturally distributed.
- **Payment Service:** Add instances. Each gets its own Kafka consumer assignment (by partition). Outbox polling instances are safe because `SELECT ... FOR UPDATE` prevents double-publish across instances.
- **Fraud Service:** Add pods. Each pod's workers join `fraud-service-group`. Max parallelism = partition count.
- **PostgreSQL:** Vertical scaling (larger instance) for writes. Add read replicas for `getPayment()` reads. Consider Citus or partitioning for >10K TPS.
- **Kafka:** Add brokers, increase partition count. Monitor consumer lag as scaling signal.
- **Redis:** Redis Cluster (hash-slot sharding) or Redis Sentinel (HA) for the shared rate limit and cache layer.

### 10.3 Caching Strategy (Redis)

| Cache Key | TTL | Purpose | Invalidation |
|---|---|---|---|
| `idem:<idempotencyKey>` | 24 hours | Avoid DB hit for idempotent replays | Natural expiry only |
| `payment:<paymentId>` | 30 minutes | Avoid DB hit for GET /payments/:id | Evicted in `processPaymentResult()` after status change |
| `rate_limit:<userId>` | 60 seconds | Sliding window counter for rate limiting | Natural expiry (window-based) |
| `velocity:<userId>` | 60 seconds | Fraud velocity counter (fraud service) | Natural expiry |

### 10.4 Backpressure Handling

| Layer | Mechanism |
|---|---|
| **API Gateway** | Redis rate limiter: token bucket (burst capacity absorbs spikes, refill rate = sustained limit). Returns 429 when exceeded. |
| **Payment Service** | Per-user rate limit (100/min). Redis Lua script is atomic — no race conditions. |
| **Kafka producer** | `linger.ms=5`, `batch.size=32768`: micro-batches small messages for throughput. Backpressure semaphore (`MAX_INFLIGHT=30`): limits outstanding sends. |
| **Kafka consumer** | `max.poll.records=10` (prod config) / `50` (app config): limits batch size per poll. `max.poll.interval.ms=300000`: 5-minute processing budget before partition reassignment. |
| **Outbox** | Batch size 50: natural backpressure — if publish rate < creation rate, outbox queue grows. Monitoring: `idx_outbox_status` index enables fast count of `NEW` events. |
| **Webhook** | Thread pool queue capacity 500. When full, `ThreadPoolTaskExecutor` rejects new tasks (webhook delivery delayed until queue drains). |
| **Fraud** | 3 consumer workers per pod. Adding pods adds workers. Kafka holds messages during overload — consumers process at their own rate. |

---

## 11. Observability

### 11.1 Logging Strategy

**Format:** Structured JSON via `logstash-logback-encoder` (payment-service) and manual JSON format strings (fraud-service).

**Payment Service log entry example:**
```json
{
  "timestamp": "2026-03-28T10:00:00.123Z",
  "level": "INFO",
  "service": "payment-service",
  "logger": "c.p.p.service.PaymentService",
  "thread": "kafka-consumer-1",
  "message": "Payment processed successfully",
  "correlationId": "abc-123-def",
  "traceId": "trace-456-ghi",
  "requestId": "req-789",
  "serviceName": "payment-service"
}
```

**MDC (Mapped Diagnostic Context) Keys:**

| Key | Set By | Propagated Via |
|---|---|---|
| `traceId` | `CorrelationIdFilter` (from `X-Trace-ID` header or generated) | Kafka header `x-trace-id` |
| `correlationId` | `CorrelationIdFilter` (from `X-Correlation-ID` header or generated) | Kafka header `x-correlation-id` |
| `requestId` | `CorrelationIdFilter` (always generated) | Not propagated (per-request) |
| `serviceName` | `CorrelationIdFilter` (hardcoded "payment-service") | Not propagated |

**Correlation ID lifecycle:**
```
Client → API Gateway (generates X-Correlation-ID if absent)
  → CorrelationIdFilter (reads from header, sets MDC)
    → PaymentService (stores correlationId in Payment entity)
      → OutboxEvent (stores correlationId + traceId explicitly)
        → Kafka header x-correlation-id
          → Fraud Service (reads from Kafka header, logs with it)
            → fraud.result Kafka header x-correlation-id
              → FraudResultConsumer (reads header, sets MDC)
                → All downstream logs carry the same correlationId
```

### 11.2 Metrics

**Payment Service (Micrometer → Prometheus):**

| Metric | Type | Tags | Purpose |
|---|---|---|---|
| `payments_created_total` | Counter | `currency` | Payment volume by currency |
| `payments_processed_total` | Counter | `currency`, `result` (success/failed/fraud) | Processing outcomes |
| `fraud_detected_total` | Counter | `reason` | Fraud detection frequency by type |
| `payment_processing_seconds` | Timer | `status` | End-to-end payment processing latency |

**Fraud Service (prometheus_client):**

| Metric | Type | Labels | Purpose |
|---|---|---|---|
| `fraud_requests_total` | Counter | `result` (fraud/clean), `stage` (rule/ml/combined) | Request volume and fraud rate |
| `fraud_request_duration_seconds` | Histogram | — | Scoring latency distribution |
| `fraud_score_distribution` | Histogram | buckets: 0.1-1.0 | ML score distribution for threshold tuning |

### 11.3 Monitoring Setup (Prometheus + Grafana)

**Prometheus:**
- Scrape interval: 15s
- Targets: `payment-service:8080/actuator/prometheus`, `fraud-service:8000/metrics`
- Retention: 15 days (`--storage.tsdb.retention.time=15d`)

**Key Grafana Dashboards to Build:**

| Dashboard | Panels |
|---|---|
| **Payment Overview** | TPS, success rate, p50/p95/p99 latency, active PENDING count |
| **Fraud Detection** | Fraud rate (rule vs ML), score distribution, velocity trigger rate |
| **Kafka Health** | Consumer lag per group, published events/s, DLQ message count |
| **Infrastructure** | DB connection pool usage, Redis hit rate, pod CPU/memory |

### 11.4 Alerting Strategy

| Alert | Condition | Severity | Response |
|---|---|---|---|
| Payment latency p95 > 500ms | `histogram_quantile(0.95, payment_create_duration) > 0.5` | Warning | Check DB connection pool, Kafka lag |
| Fraud service down | `up{job="fraud-service"} == 0` for > 2m | Critical | Circuit breaker active — payments flowing with fallback |
| Kafka consumer lag > 1000 | `kafka_consumer_lag > 1000` for > 5m | Warning | Scale consumers or check for stuck consumer |
| DLQ messages > 0 | `count(messages in *.DLQ topics) > 0` | Critical | Investigate poison messages, potential data inconsistency |
| Reconciliation anomaly | Any `reconciliation.alerts` event | Critical | Manual investigation required |
| Outbox backlog > 100 | `count(outbox_events WHERE status='NEW') > 100` | Warning | Outbox publisher may be failing; check Kafka connectivity |
| Circuit breaker OPEN | Resilience4j health indicator | Warning | Downstream service degraded |

---

## 12. Security

### 12.1 Authentication (JWT)

```
Client → API Gateway → Payment Service

1. Client sends: Authorization: Bearer <JWT>
2. API Gateway:
   - SecurityConfig.jwtDecoder(): HS256 symmetric key validation
   - NimbusReactiveJwtDecoder verifies signature + expiry locally (no network call)
   - If invalid → 401 Unauthorized (request never reaches downstream)
3. RequestEnrichmentFilter:
   - Extracts JWT `sub` claim → sets X-User-Id header
   - REMOVES Authorization header (downstream doesn't see raw JWT)
4. Payment Service trusts X-User-Id (only because it comes from the gateway on a private network)
```

**Key rotation:** Change `GATEWAY_JWT_SECRET` environment variable and redeploy both the token issuer and the gateway. For zero-downtime rotation, validate against both old and new keys during transition.

**Production upgrade path:** Switch from HS256 (symmetric) to RS256 (asymmetric) — the gateway validates with the public key (can be distributed via JWKS endpoint), and only the auth server holds the private key.

### 12.2 Authorization

- API Gateway enforces: all routes except `/actuator/health` and `/actuator/info` require valid JWT
- Payment-level authorization: the `userId` from the JWT `sub` claim is injected via `X-User-Id` — the payment service uses it as the payment owner
- Currently no role-based access control (RBAC) — all authenticated users have the same permissions

### 12.3 Rate Limiting

**Two layers:**

| Layer | Mechanism | Limit | Key |
|---|---|---|---|
| API Gateway | Redis token bucket (Spring Cloud Gateway `RequestRateLimiter`) | 100 rps global, 50 rps for `/api/payments` | Client IP (from `X-Forwarded-For` or remote address) |
| Payment Service | Redis Lua script + Caffeine fallback | 100 requests/minute per user | `userId` (from JWT) |

**Redis Lua script (atomic):**
```lua
local count = redis.call('INCR', KEYS[1])
if count == 1 then
    redis.call('EXPIRE', KEYS[1], 60)
end
if count > limit then
    return 0  -- rate exceeded
end
return 1  -- allowed
```

### 12.4 Input Validation

**Payment creation request (`CreatePaymentRequest`):**

| Field | Validation | Annotation |
|---|---|---|
| `userId` | Not null | `@NotNull` |
| `merchantId` | Not null | `@NotNull` |
| `amount` | Not null, ≥ 0.01 | `@NotNull @DecimalMin("0.01")` |
| `currency` | Not blank, exactly 3 chars | `@NotBlank @Size(min=3, max=3)` |
| `description` | Max 512 chars | `@Size(max=512)` |

Validation failures → `MethodArgumentNotValidException` → `GlobalExceptionHandler` → 400 with detailed field errors.

### 12.5 Replay Attack Protection

**Idempotency keys** serve double duty:
1. **Client-side:** ensures the same payment is only created once (network retry safety)
2. **Security:** prevents replay attacks where an attacker captures and resends a valid payment request

The idempotency key is cached for 24 hours in Redis and enforced permanently by the DB `UNIQUE` constraint. Even if the Redis TTL expires, the DB constraint prevents replay.

**Webhook HMAC signing:** Each webhook endpoint has a `secret` (generated on registration). Deliveries include headers `X-Payment-Event`, `X-Delivery-Id`, `X-Idempotency-Key` — the merchant can verify authenticity using the shared secret. (Note: HMAC signature computation on the payload body is the standard implementation pattern; the endpoint `secret` is stored for this purpose.)

---

## 13. DevOps & Deployment

### 13.1 Docker Setup

```
docker-compose.yml
├── postgres:15          (:5433)  — Volume: postgres-data
├── zookeeper:7.5.0      (:2181)
├── kafka:7.5.0          (:29092 external, :9092 internal)
├── redis:7-alpine       (:6379)  — AOF, 256MB, LRU eviction
├── fraud-service        (:8000)  — Profile: app|full
├── payment-service      (:8080)  — Profile: app|full
├── api-gateway          (:8090)  — Profile: app|full
├── prometheus:2.47.0    (:9090)  — 15d retention
└── grafana:10.1.0       (:3000)  — admin/admin
```

**Docker Profiles:**
- Default (no profile): infrastructure only (Postgres, Kafka, Zookeeper, Redis, Prometheus, Grafana)
- `app`: adds application services (fraud, payment, gateway)
- `full`: same as `app` (allows future differentiation)

**Health Checks:**
- PostgreSQL: `pg_isready -U user -d payments` (10s interval)
- Kafka: `kafka-broker-api-versions --bootstrap-server localhost:9092` (30s interval)
- Redis: `redis-cli ping` (10s interval)
- Fraud Service: `curl -f http://localhost:8000/health` (15s interval)
- Payment Service: `wget -qO- http://localhost:8080/actuator/health` (15s interval, 60s start period)
- API Gateway: `wget -qO- http://localhost:8090/actuator/health` (15s interval, 30s start period)

**Startup Order (enforced via `depends_on` + `condition: service_healthy`):**
```
postgres ──┐
kafka   ───┤
redis   ───┼──► fraud-service ──┐
           │                    ├──► payment-service ──► api-gateway
           └────────────────────┘
```

### 13.2 Service Isolation

- Each service runs in its own container with dedicated port mapping
- Network isolation via Docker Compose default bridge network
- Inter-service communication uses Docker DNS names (`kafka:9092`, `redis:6379`, `fraud-service:8000`)
- External access via mapped ports (host machine only)
- Kafka dual listener setup: `INTERNAL://kafka:9092` for containers, `EXTERNAL://localhost:29092` for host development

### 13.3 CI/CD Pipeline Overview

Recommended pipeline (not yet implemented in-repo):

```
┌─────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐
│  Commit  │──▶│  Build   │──▶│  Test    │──▶│  Image   │──▶│  Deploy  │
│  Push    │   │  Compile │   │  Unit    │   │  Build   │   │  Stage   │
└─────────┘   │  Lint    │   │  Integ   │   │  Push    │   │  Prod    │
              └──────────┘   │  k6 Load │   └──────────┘   └──────────┘
                             └──────────┘
```

- **Build:** Maven for Java services, pip for fraud service, npm for frontend
- **Test:** JUnit 5 (H2 in-memory), pytest (fraud service), k6 load tests
- **Image:** Multi-stage Docker builds (build stage → slim runtime)
- **Deploy:** Rolling update with health check gates

### 13.4 Local Development Setup

```bash
# Start infrastructure
docker compose up -d

# Start application services locally (for development/debugging)
cd payment-service && ./mvnw spring-boot:run
cd fraud-service && uvicorn main:app --reload --port 8000
cd api-gateway && ./mvnw spring-boot:run
cd frontend && npm run dev

# OR start everything in Docker
docker compose --profile full up -d

# Run load tests
k6 run load-tests/payment-load-test.js
```

**Test Configuration:**
- Payment service tests use H2 in-memory DB (auto-configured via `test/resources/application.yml`)
- Kafka and Redis are excluded from test context (`spring.autoconfigure.exclude`)
- Rate limiting disabled for tests (`app.rate-limiting.enabled: false`)

---

## 14. Trade-offs and Alternatives

### 14.1 Why Kafka vs RabbitMQ

| Dimension | Kafka (chosen) | RabbitMQ |
|---|---|---|
| **Message retention** | Retained for configurable period (7 days) — consumers can replay | Messages deleted after acknowledgment |
| **Consumer model** | Pull-based — consumers control their pace | Push-based — broker controls delivery rate |
| **Throughput** | ~1M messages/s per broker | ~50K messages/s per node |
| **Ordering** | Per-partition ordering guarantees | Per-queue ordering (stricter but less scalable) |
| **Replay capability** | Reset consumer offset to replay historical events | Not possible without dead-letter re-routing |
| **Exactly-once** | Idempotent producer + transactional consumer | Built into AMQP (but different guarantee model) |

**Why Kafka wins here:**
- Payment events need **retention** for replay, reconciliation, and debugging
- **Partition-based ordering** with `userId` key ensures per-user consistency at scale
- **Consumer group** model makes horizontal scaling trivial (add consumers ≤ partition count)
- **High throughput** needed for burst scenarios (up to 5K TPS)
- **Event sourcing** compatibility — the Kafka topic is effectively the immutable event log

**When RabbitMQ would be better:**
- Complex routing (headers, topics, fanout exchanges) needed
- Message priority or delayed message patterns
- Lower operational overhead for small-scale deployments
- True message-level TTL with automatic dead-lettering

### 14.2 Why PostgreSQL vs NoSQL

| Dimension | PostgreSQL (chosen) | MongoDB / DynamoDB |
|---|---|---|
| **ACID transactions** | Full support — critical for ledger + outbox | MongoDB 4.0+ multi-doc TX (limited); DynamoDB single-item |
| **Schema enforcement** | Strong schema with constraints, indexes | Schema-less (flexible but dangerous for financial data) |
| **Joins** | Efficient SQL joins for reconciliation queries | Application-level joins or denormalization |
| **Consistency** | Strong consistency by default | DynamoDB: eventual by default (strongly consistent reads cost 2x) |
| **Ledger suitability** | NUMERIC(19,4) with exact arithmetic | Double-precision floats (rounding errors) unless using Decimal128 |

**Why PostgreSQL wins here:**
- **Ledger requires exact arithmetic** — NUMERIC type prevents floating-point rounding errors
- **Transactional outbox pattern** depends on writes to multiple tables in a single ACID transaction
- **Reconciliation queries** use complex aggregations (SUM, GROUP BY, HAVING) that are natural in SQL
- **UNIQUE constraints** and `SELECT ... FOR UPDATE` are essential for idempotency enforcement
- **Mature ecosystem** — battle-tested for financial applications

**When NoSQL would be better:**
- Write-heavy workloads at >50K TPS (DynamoDB auto-scales writes)
- Document-oriented data without relational integrity requirements
- Geographic distribution requiring multi-region active-active writes

### 14.3 Why Saga vs 2PC (Two-Phase Commit)

| Dimension | Saga (chosen) | 2PC |
|---|---|---|
| **Availability** | Each service operates independently; partial failure tolerated | Coordinator failure blocks all participants |
| **Latency** | Async — no waiting for all participants to vote | Synchronous — all participants must respond before commit |
| **Complexity** | Compensation logic per step | Simpler conceptually but harder operationally |
| **Scaling** | Each service scales independently | Coordinator is a bottleneck |
| **Data locality** | Each service owns its data | Distributed lock holding required |

**Why Saga wins here:**
- Our services (payment, fraud, ledger) are **independent microservices** — 2PC would require a distributed transaction coordinator that defeats the purpose of decomposition
- **Fraud detection is async** by design — 2PC can't span a Kafka-based async pipeline
- **Compensation is natural**: if fraud check fails, set status to `FRAUD_REJECTED`; if ledger fails, don't update status (retry or DLQ)
- **No long-held locks**: 2PC holds locks on all participants during the prepare phase — devastating for throughput

### 14.4 Why REST vs gRPC

| Dimension | REST/JSON (chosen) | gRPC/Protobuf |
|---|---|---|
| **Tooling** | Universal — curl, browser, Postman, any language | Requires proto compiler, generated stubs |
| **Payload size** | ~500B JSON (human-readable) | ~150B Protobuf (binary, smaller) |
| **Latency** | HTTP/1.1 overhead per request | HTTP/2 multiplexing, lower overhead |
| **Schema evolution** | No built-in versioning (manual v1/v2) | Proto field numbering handles backward compatibility |
| **Streaming** | WebSocket (separate protocol) | Built-in bidirectional streaming |

**Why REST wins here:**
- **Cross-language simplicity**: payment-service (Java), fraud-service (Python), frontend (TypeScript) — all speak HTTP/JSON natively
- **Debugging ease**: JSON payloads are human-readable in logs, Kafka messages, and debugging tools
- **Gateway compatibility**: Spring Cloud Gateway is built for HTTP routing
- **Client integration**: merchants integrating our webhook/API expect REST (industry standard for payment APIs)

**When gRPC would be better:**
- Internal service mesh with 10+ services and strict latency requirements
- High-frequency inter-service calls (10K+ RPC/s between two services)
- Need for bidirectional streaming (e.g., real-time ledger updates)

---

## 15. Interview Talking Points

### "Design a payment system"

> The system is built on an event-driven architecture with five key components: an API Gateway for auth and rate limiting, a Payment Service as the write-path orchestrator, a Fraud Service for async ML-based scoring, a double-entry Ledger for financial integrity, and Kafka as the event backbone.
>
> A payment creation goes through three phases: (1) synchronous ingestion — validate, deduplicate via idempotency key, write Payment + OutboxEvent in a single DB transaction, return 201 PENDING immediately; (2) async fraud check — outbox publisher polls every 1s and pushes to Kafka, fraud service scores with rule engine + IsolationForest, returns result via Kafka; (3) async settlement — FraudResultConsumer writes double-entry ledger, verifies SUM(DEBIT)==SUM(CREDIT), transitions to SUCCESS or FRAUD_REJECTED, dispatches webhook.
>
> The critical design decision is using the Transactional Outbox pattern for DB-to-Kafka consistency. We never publish to Kafka inside the transaction that writes the payment — instead, we write the event to an outbox table in the same transaction, and a separate poller publishes it. This guarantees at-least-once delivery without distributed transactions.

### "How do you handle duplicate requests?"

> Three layers of idempotency defense. First, a Redis cache on the idempotency key (24h TTL) — cheapest check, serves the common case of client retries within seconds. Second, a PostgreSQL `SELECT ... FOR UPDATE` on the idempotency_key column — serializes concurrent requests and handles cache misses. Third, a `UNIQUE` constraint on `idempotency_key` — database-level invariant that catches any remaining races.
>
> For Kafka consumers, we use a `processed_events` table with a composite primary key `<topic>:<partitionKey>:<eventId>`. The INSERT is inside the same transaction as the business logic. If a consumer crashes after processing but before acking, Kafka redelivers the message — the PK insert fails, and we skip. If the transaction rolled back (business logic failed), the PK insert also rolled back, so we correctly reprocess.

### "How do you ensure consistency?"

> We have a dual consistency model. The ledger is strongly consistent — DEBIT and CREDIT entries are written in the same transaction, and `validateInvariant()` verifies `SUM(DEBIT) == SUM(CREDIT)` before allowing the transaction to commit. Any mismatch rolls back the entire transaction and retries 3 times before going to DLQ.
>
> Payment status transitions are eventually consistent — there's a 1-3 second window between payment creation (PENDING) and final status (SUCCESS/FAILED). This is acceptable because the client gets an immediate 201 with the payment ID and can poll or subscribe via WebSocket. The reconciliation service runs every 30 minutes as defense-in-depth, catching stale payments, ghost successes, ledger imbalances, and orphan entries.
>
> DB-to-Kafka consistency is guaranteed by the outbox pattern — no dual-write problem because we never write to Kafka inside a DB transaction. The outbox event is part of the payment transaction; a separate poller handles Kafka publishing with retries.

### "How does your system scale?"

> Every service is stateless and horizontally scalable. The Payment Service scales by adding instances — each gets its own Kafka consumer partition assignment (up to partition count). The Fraud Service scales by adding pods — each pod runs 3 consumer threads that join the same consumer group. The outbox publisher is safe across instances because it uses `SELECT ... FOR UPDATE` to prevent double-publishing.
>
> The main bottleneck at scale is PostgreSQL writes. Mitigations: HikariCP pool of 20 connections, Hibernate batch inserts (batch_size=50, ordered), and the outbox polling batch (50 per poll). Beyond 5K TPS, we'd partition the database (by merchant or user), add read replicas for the GET path, and increase Kafka partition counts. Redis is already cluster-ready and handles 100K+ ops/s for caching and rate limiting.

---

## 16. What Could Be Improved Further

> **Note:** Several items from the original improvement list have been implemented.
> See the changelog below for details.

### 16.0 Recently Implemented Upgrades

| Upgrade | Description | Files Changed |
|---|---|---|
| **Account System** | `Account` entity with `@Version` optimistic locking. `AccountService` manages balance debit/credit. `LedgerService` now updates account balances atomically within the ledger transaction. | `Account.java`, `AccountType.java`, `AccountRepository.java`, `AccountService.java`, `LedgerService.java` |
| **System-Level Backpressure** | `BackpressureService` polls Kafka consumer lag via AdminClient. Adaptive rate limiting: normal (1.0x), critical lag >5K (0.5x), severe lag >10K (0.1x). Integrated into `RateLimiterService`. | `BackpressureService.java`, `RateLimiterService.java`, `application.yml` |
| **Composite Partition Keys** | Outbox publisher now uses `userId:saltBucket` composite keys where `saltBucket = hash(paymentId) % saltFactor`. Prevents hot partitions from high-volume users. | `OutboxPublisherService.java`, `application.yml` (`partition-salt-factor: 3`) |
| **ML Model Versioning** | `ModelManager` with versioned models (v1: 3 features, v2: 11 features). `FeatureExtractor` with `FeatureStore` (Redis-backed). Hot-reload support via `/admin/model/train-v2`. | `model_manager.py`, `feature_engineering.py`, `feature_store.py`, `main.py` |
| **Enhanced Load Testing** | Four k6 profiles: default, stress (1500 VUs), soak (30min), spike (2000 VUs). Throughput and backpressure metrics. | `payment-load-test.js` |
| **Failure Simulation** | Bash scripts for Kafka, Redis, PostgreSQL, fraud service, and network partition failures. | `scripts/failure-simulation/` |
| **API Gateway Versioning** | `/v1/payments/**` versioned route alongside legacy `/api/payments/**`. `RequestValidationFilter` validates Content-Type, Idempotency-Key, and body size at gateway level. | `application.yml`, `RequestValidationFilter.java` |
| **Observability Upgrade** | New metrics: `outbox_events_published_total`, `outbox_events_failed_total`, `account_balance_updates_total`, `backpressure_activations_total`, `active_payments_pending` gauge, `kafka_consumer_lag_total`, `kafka_consumer_lag_max_partition`. | `PaymentMetrics.java`, `BackpressureService.java` |
| **Documentation** | `docs/performance.md` — latency breakdown, tuning guide, capacity planning. `docs/failure-scenarios.md` — failure classification, recovery procedures, alerting thresholds. | `docs/performance.md`, `docs/failure-scenarios.md` |

### 16.1 Short-Term Improvements

| Area | Improvement | Impact |
|---|---|---|
| **Outbox throughput** | Switch from polling to CDC (Debezium) — eliminates 1s latency, removes batch-size ceiling | 10x throughput, sub-100ms event latency |
| **HMAC webhook signatures** | Actually compute HMAC-SHA256 over the payload body using the endpoint secret | Security — merchants can verify webhook authenticity |
| **RBAC** | Add role-based access control (admin, merchant, user) with JWT claims | Authorization — currently all authenticated users are equal |
| **Idempotency key scoping** | Scope keys per-merchant or per-user to prevent cross-tenant collisions | Multi-tenancy safety |
| **Structured error codes** | Adopt RFC 7807 Problem Details for all error responses | Better client integration |

### 16.2 At 10x Scale (50K TPS)

| Challenge | Solution |
|---|---|
| **Single PostgreSQL** | Shard by `merchantId` (Citus); separate read replicas for queries; ledger write-ahead split (write to fast store, async reconcile to cold store) |
| **Kafka partition limit** | Increase to 50-100 partitions per topic; consider topic compaction for payment status topics |
| **Outbox polling** | Replace with Debezium CDC — captures `outbox_events` inserts as a Postgres WAL stream, publishes to Kafka directly. Eliminates polling overhead and reduces latency to ~100ms. |
| **Fraud ML** | Move from scikit-learn to a GPU-accelerated model serving framework (TensorFlow Serving, NVIDIA Triton). Feature store (Feast) for real-time features. Online learning for model adaptation. |
| **Redis** | Redis Cluster with 6+ nodes. Separate clusters for rate limiting (write-heavy) and caching (read-heavy). |
| **Multi-region** | Active-passive with Kafka MirrorMaker 2 for cross-region replication. DNS failover for API Gateway. Region-local fraud scoring. |
| **Event schema** | Adopt Avro or Protobuf with a Schema Registry. JSON at 50K TPS generates significant serialization overhead. |
| **Observability** | OpenTelemetry SDK for distributed tracing (replace manual correlationId propagation). Centralized log aggregation (ELK or Loki). |
| **Testing** | Chaos engineering (Litmus, Chaos Monkey). Automated reconciliation verification in staging. Shadow traffic for fraud model testing. |

### 16.3 Architecture Evolution Path

```
Current                          Near-term                        Long-term
──────                          ─────────                        ─────────
Outbox polling (1s)         →   Debezium CDC (<100ms)        →   Event sourcing (Axon/EventStore)
JSON/Kafka                  →   Avro + Schema Registry       →   Protobuf + gRPC internal
IsolationForest             →   XGBoost + feature store      →   Deep learning + online learning
Single PostgreSQL           →   Read replicas + Citus        →   Sharded + CQRS separation
Manual correlation IDs      →   OpenTelemetry auto-instrumentation
Redis single instance       →   Redis Sentinel              →   Redis Cluster
Docker Compose              →   Kubernetes + Helm            →   Service mesh (Istio)
```
