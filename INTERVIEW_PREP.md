# Distributed Payments System — Interview Preparation Guide

> **Purpose**: Deep-dive reference for system design and backend engineering interviews.  
> **Audience**: Senior backend / system design interviewers.  
> **Based on**: Production-grade codebase with 65+ Java classes, Python ML fraud service, and full observability stack.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [High-Level Architecture](#2-high-level-architecture)
3. [Tech Stack and Rationale](#3-tech-stack-and-rationale)
4. [Detailed Component Breakdown](#4-detailed-component-breakdown)
5. [Data Layer Design](#5-data-layer-design)
6. [Key Features and Implementation Details](#6-key-features-and-implementation-details)
7. [Scalability and Performance](#7-scalability-and-performance)
8. [Reliability and Fault Tolerance](#8-reliability-and-fault-tolerance)
9. [Security Considerations](#9-security-considerations)
10. [Observability](#10-observability)
11. [Deployment and DevOps](#11-deployment-and-devops)
12. [Trade-offs and Design Decisions](#12-trade-offs-and-design-decisions)
13. [Interview Questions and Answers](#13-interview-questions-and-answers)
14. [Elevator Pitch](#14-elevator-pitch)
15. [Resume Bullet Mapping](#15-resume-bullet-mapping)

---

## 1. Project Overview

### Problem Statement

Payment processing systems must handle money movement with zero data loss, exactly-once semantics, and real-time fraud detection — all while maintaining sub-second latency at thousands of transactions per second. Most payment failures are silent: double-charges, lost events, and unbalanced ledgers. Building a system that is provably correct under partial failure is a hard distributed systems problem.

### Why This Project Was Built

- Demonstrates production-grade patterns used at Stripe, Adyen, and Square: transactional outbox, double-entry ledger, idempotent consumers, circuit breakers
- Integrates real-time ML fraud detection (IsolationForest with feature engineering) into an event-driven pipeline
- Covers the full infrastructure stack: API gateway with JWT auth, Kafka event streaming, Redis caching with fallbacks, PostgreSQL with optimistic locking, Prometheus/Grafana observability
- Designed to withstand any single component failure without data loss or inconsistency

### Key Objectives and Success Criteria

| Objective | Success Criteria |
|-----------|-----------------|
| **Zero payment loss** | Transactional outbox guarantees DB→Kafka consistency; `acks=all` producer; DLQ for poison messages |
| **Exactly-once processing** | `processed_events` dedup table + idempotent Kafka producer + payment status guard |
| **Ledger consistency** | Double-entry invariant enforced: `SUM(DEBIT) == SUM(CREDIT)` per payment, verified before SUCCESS |
| **Fraud detection < 200ms** | Two-stage pipeline: rule engine (~1ms) + IsolationForest ML (~5ms); Kafka round-trip dominates |
| **p95 latency < 500ms** | Payment creation: ~10ms p50, ~50ms p95 under load |
| **1,000+ TPS sustained** | Validated via k6 stress profile (1,500 VUs); adaptive backpressure prevents collapse |
| **Graceful degradation** | Every external dependency (Redis, Kafka, Fraud Service) has a fallback path |

---

## 2. High-Level Architecture

### System Design Explanation

The system follows an **event-driven microservices architecture** with a synchronous write path and asynchronous processing pipeline. The payment creation is synchronous (client gets an immediate `201 Created`), but all downstream processing — fraud detection, ledger recording, webhook delivery — happens asynchronously via Kafka, decoupling latency from processing complexity.

The **Transactional Outbox Pattern** is the linchpin: payment state and Kafka events are written in the same database transaction, eliminating the dual-write problem where a service updates its DB but fails to publish to Kafka (or vice versa).

### Components and Their Responsibilities

| Component | Tech | Responsibility |
|-----------|------|---------------|
| **API Gateway** | Spring Cloud Gateway (Java 17) | JWT auth, Redis rate limiting, request enrichment, circuit breaker, retry |
| **Payment Service** | Spring Boot 3.4.4 (Java 17) | Payment lifecycle orchestration, outbox pattern, ledger, idempotency, caching, reconciliation |
| **Fraud Service** | FastAPI (Python 3.11) | Rule engine + IsolationForest ML scoring via Kafka pipeline |
| **PostgreSQL 15** | RDBMS | Source of truth: payments, outbox_events, ledger_entries, processed_events, accounts, webhooks |
| **Apache Kafka** | Event streaming | Async event bus: payment.created → fraud.request → fraud.result → payment.processed |
| **Redis 7** | Cache + rate limiting | Idempotency cache, payment cache, rate limiting (Lua scripts), WebSocket pub/sub, fraud velocity tracking |
| **Prometheus** | Metrics | Scrapes payment-service, fraud-service, api-gateway every 15s |
| **Grafana** | Dashboards | 10-panel auto-provisioned dashboard: throughput, latency, Kafka lag, fraud rates |
| **Frontend** | React + TypeScript + Vite | Real-time dashboard with WebSocket updates |

### Architecture Diagram

```
                              ┌─────────────────────────────────────────┐
                              │         Client / k6 / Frontend          │
                              └──────────────┬──────────────────────────┘
                                             │ HTTPS + JWT + Idempotency-Key
                                             ▼
                              ┌─────────────────────────────────────────┐
                              │    API Gateway  (Spring Cloud · :8090)  │
                              │    JWT auth · Redis rate limit · CB     │
                              │    X-Correlation-ID · X-User-Id inject  │
                              └──────────────┬──────────────────────────┘
                                             │ HTTP (auth header stripped)
                                             ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                    Payment Service  (Spring Boot 3.4.4 · :8080)                  │
│                                                                                  │
│  ┌───────────────┐    ┌──────────────┐    ┌───────────────┐    ┌──────────────┐  │
│  │PaymentController│──▶│PaymentService │──▶│OutboxPublisher│──▶│  Kafka       │  │
│  │POST/GET /pay   │    │@Transactional │    │@Scheduled 1s  │    │  acks=all   │  │
│  └───────────────┘    └──────┬───────┘    └───────────────┘    └──────────────┘  │
│                              │                                                   │
│  ┌───────────────────────────┼───────────────────────────────────────────────┐    │
│  │             Kafka Consumers (3 concurrency, manual ack)                   │    │
│  │  PaymentEventConsumer ◀── payment.created → fraud.request                │    │
│  │  FraudResultConsumer  ◀── fraud.result   → processPaymentResult()        │    │
│  │  PaymentProcessedConsumer ◀── payment.processed → Webhook + WebSocket    │    │
│  └───────────────────────────────────────────────────────────────────────────┘    │
│                                                                                  │
│  ┌─────────────┐  ┌────────────────┐  ┌───────────────────┐  ┌───────────────┐  │
│  │LedgerService │  │Reconciliation  │  │IdempotentConsumer │  │  PaymentCache │  │
│  │double-entry  │  │@Cron 30min     │  │PK-based dedup     │  │  Redis+evict  │  │
│  └─────────────┘  └────────────────┘  └───────────────────┘  └───────────────┘  │
│                                                                                  │
│  PostgreSQL: payments · outbox_events · ledger_entries · processed_events         │
│  Redis: rate_limit:* · idem:* · payment:* · ws:payments pub/sub                  │
└──────────────────────────────────────────────────────────────────────────────────┘
                         │ Kafka: fraud.request
                         ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                    Fraud Service  (FastAPI · Python 3.11 · :8000)                │
│                                                                                  │
│  Stage 1: Rule Engine (velocity >10/60s, amount >$10K, currency allowlist)       │
│  Stage 2: ML Model (IsolationForest v1: 3 features, v2: 11 features)            │
│  FeatureStore (Redis) · FeatureExtractor · ModelManager (versioned, hot-reload)  │
└──────────────────────────────────────────────────────────────────────────────────┘
                         │ Kafka: fraud.result
                         ▼
                    FraudResultConsumer → LedgerService → Payment SUCCESS/FAILED
```

### Data Flow — Step-by-Step Request Lifecycle

**Happy path (payment creation → fraud check → ledger → success):**

```
1. Client → POST /v1/payments (JWT Bearer + Idempotency-Key header)

2. API Gateway (:8090):
   a. Validate JWT (HS256 symmetric key, local verification — no auth server)
   b. Redis token-bucket rate limit (100 rps global, 50 rps for /payments)
   c. Inject X-Correlation-ID (UUID) + X-User-Id (from JWT sub claim)
   d. Strip Authorization header
   e. Forward to payment-service:8080

3. Payment Service (PaymentController → PaymentService.createPayment):
   a. Rate limit check — Redis Lua INCR+EXPIRE; Caffeine fallback if Redis down
   b. Idempotency check — Redis GET idem:<key> (fast path)
   c. If cache miss → SELECT ... FOR UPDATE on idempotency_key (pessimistic lock)
   d. If new: INSERT Payment(status=PENDING) + OutboxEvent in SAME @Transactional
   e. Cache idempotency key (24h TTL) + payment (30min TTL) in Redis
   f. Return 201 Created with PaymentResponse

4. OutboxPublisherService (@Scheduled every 1s):
   a. SELECT TOP 50 FROM outbox_events WHERE status='NEW'
   b. Build ProducerRecord with Kafka headers (x-event-id, x-correlation-id, x-trace-id)
   c. Composite partition key: userId:hash(paymentId)%3
   d. kafkaTemplate.send() → on success: mark PUBLISHED; on failure: retry up to 3x

5. Kafka: payment.created → PaymentEventConsumer:
   a. Deserialize PaymentEvent, extract trace headers
   b. Build FraudRequestEvent, publish to fraud.request with headers forwarded
   c. Manual ack

6. Fraud Service (Kafka consumer thread):
   a. Stage 1 — Rule engine: currency allowlist → amount >$10K → velocity >10tx/60s
   b. Stage 2 — ML: FeatureExtractor → IsolationForest.score() → normalize to [0,1]
   c. Publish FraudResultEvent to fraud.result with trace headers

7. Kafka: fraud.result → FraudResultConsumer:
   a. PaymentService.processPaymentResult() — @Transactional + @Retryable (3x)
   b. IdempotentConsumerService.tryMarkProcessed(eventId) — PK dedup
   c. If fraud=true:  status → FRAUD_REJECTED, write fraud.alerts + payment.failed
   d. If fraud=false:
      i.   LedgerService.recordPayment() — DEBIT + CREDIT entries
      ii.  LedgerService.verifyLedgerBalance() — SUM(DEBIT)==SUM(CREDIT) check
      iii. AccountService.debit(user) + credit(merchant) — optimistic locking
      iv.  Status → SUCCESS, write payment.processed outbox event
   e. Evict Redis cache, manual ack

8. Kafka: payment.processed → PaymentProcessedConsumer:
   a. WebhookService.dispatchEvent() — @Async, POST with HMAC signature
   b. WebSocketBroadcaster.broadcast() — fire-and-forget, real-time UI update
```

---

## 3. Tech Stack and Rationale

### Java 17 + Spring Boot 3.4.4

| Aspect | Detail |
|--------|--------|
| **What** | Core backend runtime for Payment Service and API Gateway |
| **Why chosen** | Mature ecosystem for financial services; strong typing prevents runtime errors in money handling; Spring Boot provides production-ready DI, transaction management, Kafka integration; `BigDecimal` for precise currency arithmetic |
| **Trade-offs** | Higher memory footprint vs Go/Rust; JVM warm-up latency; verbose compared to Python |
| **Alternatives rejected** | **Go**: faster cold start, lower memory, but weaker ORM/transaction support for complex DB workflows. **Node.js**: single-threaded, less suited for CPU-bound ledger validation. **Kotlin**: considered, but Java 17 records + Spring Boot is more widely understood in interviews |

### Python 3.11 + FastAPI (Fraud Service)

| Aspect | Detail |
|--------|--------|
| **What** | ML-powered fraud detection microservice |
| **Why chosen** | Best-in-class ML ecosystem (scikit-learn, numpy); FastAPI is async-native with auto-generated OpenAPI docs; natural fit for data science/ML workloads |
| **Trade-offs** | GIL limits true parallelism; slower than JVM for pure computation; separate runtime adds deployment complexity |
| **Alternatives rejected** | **Java ML (Weka/DL4J)**: weaker ML ecosystem, harder model iteration. **TensorFlow Serving**: overkill for IsolationForest. **ONNX Runtime on JVM**: possible but harder to iterate on feature engineering |

### PostgreSQL 15

| Aspect | Detail |
|--------|--------|
| **What** | Primary RDBMS — source of truth for all payment data |
| **Why chosen** | ACID transactions critical for financial data; `SELECT ... FOR UPDATE` for idempotency; strong indexing; mature replication for read scaling; JSONB for future schema flexibility |
| **Trade-offs** | Single-master write bottleneck at extreme scale; harder to shard than NoSQL |
| **Alternatives rejected** | **MySQL**: weaker isolation level defaults, less advanced indexing. **CockroachDB**: distributed SQL handles sharding but higher write latency. **MongoDB**: no ACID transactions across collections (pre-4.4, and still weaker guarantees) |

### Apache Kafka (Confluent 7.5)

| Aspect | Detail |
|--------|--------|
| **What** | Distributed event streaming platform |
| **Why chosen** | Durable message ordering per partition; exactly-once producer semantics (`enable.idempotence=true`); consumer group model for horizontal scaling; 7-day retention for replay; built-in partitioning for parallelism |
| **Trade-offs** | Operational complexity (ZooKeeper dependency in 7.5); eventual consistency; no native request-reply pattern |
| **Alternatives rejected** | **RabbitMQ**: simpler but no log compaction, weaker ordering guarantees, no replay. **AWS SQS**: managed but no partition ordering, higher latency. **Pulsar**: technically superior in some areas but smaller ecosystem and community |

### Redis 7

| Aspect | Detail |
|--------|--------|
| **What** | In-memory data store for caching, rate limiting, pub/sub |
| **Why chosen** | Sub-millisecond reads; Lua script atomicity for rate limiting; pub/sub for WebSocket broadcast across instances; versatile data structures (strings, sorted sets, lists) |
| **Trade-offs** | Volatile by default (mitigated with AOF persistence); single-threaded (mitigated with connection pooling); adds dependency |
| **Alternatives rejected** | **Memcached**: no Lua scripts, no pub/sub, no sorted sets for velocity tracking. **Hazelcast**: JVM-only, heavier for simple caching. **Application-level caching only (Caffeine)**: not shared across instances |

### Spring Cloud Gateway

| Aspect | Detail |
|--------|--------|
| **What** | API gateway built on Project Reactor (Netty) |
| **Why chosen** | Non-blocking I/O for high throughput; native Spring Security integration; built-in Redis rate limiter, circuit breaker, retry filters; same language as payment service (unified team) |
| **Trade-offs** | Reactive programming model has steeper learning curve; harder to debug than servlet-based gateways |
| **Alternatives rejected** | **Kong**: more features but requires separate Lua/Go runtime and database. **Envoy**: excellent L7 proxy but no native Spring integration. **AWS API Gateway**: managed but vendor lock-in, cold start on Lambda integration |

### Prometheus + Grafana

| Aspect | Detail |
|--------|--------|
| **What** | Metrics collection and visualization |
| **Why chosen** | De facto standard for containerized workloads; pull-based model scales well; PromQL is powerful; Grafana dashboards auto-provisioned via JSON config |
| **Trade-offs** | Local storage not durable at scale (production uses Thanos/Cortex); no built-in distributed tracing |
| **Alternatives rejected** | **Datadog**: excellent but expensive and vendor-locked. **ELK Stack**: better for logs than metrics. **InfluxDB**: good alternative but smaller ecosystem in Kubernetes world |

---

## 4. Detailed Component Breakdown

### 4.1 Payment Service

**Responsibilities**: Payment lifecycle management (PENDING → SUCCESS/FAILED/FRAUD_REJECTED), idempotency enforcement, outbox event creation, Kafka consumer orchestration, double-entry ledger, account balance management, webhook delivery, scheduled reconciliation.

**Internal Design**:

```
PaymentController (REST)
  └── PaymentService (@Transactional, @Retryable)
        ├── RateLimiterService (Redis Lua + Caffeine fallback)
        ├── PaymentCacheService (Redis read-through cache)
        ├── PaymentRepository (JPA + pessimistic locking)
        ├── OutboxPublisherService (@Scheduled poller → Kafka)
        ├── LedgerService (double-entry bookkeeping)
        │     └── AccountService (optimistic locking via @Version)
        ├── IdempotentConsumerService (PK dedup in processed_events)
        └── PaymentMetrics (Micrometer/Prometheus counters, gauges, timers)

Kafka Consumers:
  ├── PaymentEventConsumer (payment.created → fraud.request)
  ├── FraudResultConsumer (fraud.result → processPaymentResult)
  └── PaymentProcessedConsumer (payment.processed → Webhook + WebSocket)
```

**Key Patterns**:
- **Constructor injection everywhere** — no `@Autowired` field injection; improves testability
- **`@Transactional` boundaries** — READ_COMMITTED isolation for payment creation; MANDATORY propagation for dedup inserts
- **`@Retryable`** — 3 attempts with exponential backoff (200ms × 2) for `TransientDataAccessException`
- **`@Recover`** — both specific (`TransientDataAccessException`) and generic (`Exception`) recovery methods to avoid `ExhaustedRetryException`

**Concurrency Model**:
- 3 Kafka consumer threads per topic (configurable via `spring.kafka.listener.concurrency`)
- Backpressure semaphore: 30 max in-flight messages (3 threads × 10 max-poll-records)
- HikariCP: 20 connections, 5 minimum idle
- Webhook delivery via `@Async("webhookExecutor")` thread pool
- WebSocket broadcast via Redis pub/sub (cross-instance)

**Failure Handling**:
- DB down → HikariCP reconnects; Spring Retry handles transient errors; Kafka consumers pause and retry
- Redis down → rate limiter falls back to Caffeine (50K entries, LRU, 2min TTL); cache misses fall through to DB
- Kafka consumer crash → unacked messages redelivered; `processed_events` PK dedup prevents double-processing
- Optimistic lock conflict → Spring Retry retries the entire `@Transactional` method

### 4.2 Fraud Service

**Responsibilities**: Two-stage fraud detection pipeline (rule engine + ML), feature engineering, model versioning with hot-reload, Kafka consumer/producer, velocity tracking.

**Internal Design**:

```
main.py (FastAPI + Kafka consumer threads)
  ├── Rule Engine
  │     ├── Currency allowlist check
  │     ├── Amount threshold ($10K)
  │     └── Velocity check (>10 tx/60s via Redis or in-process deque)
  ├── FeatureExtractor → FeatureVector (11 features for v2, 3 for v1)
  │     └── FeatureStore (Redis-backed velocity, behavioral, risk features)
  └── ModelManager
        ├── IsolationForest v1 (3 features, 1000 synthetic samples)
        ├── IsolationForest v2 (11 features, enhanced pipeline)
        └── Hot-reload via /admin/model endpoints (X-Admin-Key auth)
```

**Key Algorithms**:
- **IsolationForest**: Unsupervised anomaly detection. Isolates anomalies by randomly partitioning features. Anomalies require fewer splits → lower `decision_function` scores → higher fraud probability after normalization
- **Scoring normalization**: `score = max(0, min(1, 0.5 - raw_score))` maps sklearn's decision function to [0,1] probability
- **Feature count mismatch handling**: If model expects 3 features but gets 11, first 3 are used; if 11 expected but 3 provided, zeros are padded

**Concurrency Model**:
- 3 Kafka consumer worker threads (confluent-kafka)
- Thread-safe model swap via atomic reference + `threading.Lock` on write path
- In-process fallback velocity tracking uses `deque` (thread-safe for append/popleft)

### 4.3 API Gateway

**Responsibilities**: JWT authentication, Redis rate limiting, request enrichment (correlation ID, user ID), circuit breaker, retry on 5xx, CORS configuration.

**Internal Design**:

```
SecurityConfig (@EnableWebFluxSecurity)
  └── JWT validation (HS256, NimbusReactiveJwtDecoder)

Global Filters (ordered):
  -2  RequestValidationFilter  — Content-Type, Idempotency-Key, body size
  -1  RequestEnrichmentFilter  — X-Correlation-ID, X-User-Id, strip Authorization

Route Filters:
  ├── RequestRateLimiter (Redis token bucket, remoteAddrKeyResolver)
  ├── Retry (3x on 502/503/504, GET only, exponential backoff)
  ├── CircuitBreaker (fallback to /fallback/payment-service)
  └── StripPrefix (versioned routes: /v1/payments → /payments)
```

**Security Notes**:
- **X-Forwarded-For intentionally NOT trusted** — prevents IP spoofing for rate limiting
- **Authorization header stripped** before forwarding — downstream trusts only `X-User-Id` injected by gateway
- **Path matching uses regex** (`^(/api|/v1)?/payments/?$`) — not `String.contains()`

### 4.4 Outbox Publisher

**Responsibilities**: Polls `outbox_events` table every 1 second, publishes to Kafka with exactly-once producer semantics, marks events as PUBLISHED or FAILED after max retries.

**Key Design Decisions**:
- **Polling interval**: 1 second — balances latency vs DB load
- **Batch size**: 50 events per poll — prevents one slow poll from starving others
- **Composite partition key**: `userId:hash(paymentId) % saltFactor(3)` — distributes hot users across 3 partitions while maintaining bounded affinity
- **Trace IDs stored as explicit columns** — publisher never parses JSON payload, decouples from schema evolution
- **Kafka headers**: `x-event-id`, `x-correlation-id`, `x-trace-id` — standardized via `KafkaHeaderUtil`

---

## 5. Data Layer Design

### Database Schema

```sql
-- Core payment record
payments (
    id              UUID PRIMARY KEY,
    user_id         UUID NOT NULL,           -- INDEX
    merchant_id     UUID NOT NULL,
    amount          DECIMAL(19,4) NOT NULL,
    currency        VARCHAR(3) NOT NULL,
    status          VARCHAR(20) NOT NULL,    -- INDEX (PENDING|SUCCESS|FAILED|FRAUD_REJECTED)
    idempotency_key VARCHAR UNIQUE NOT NULL,
    description     VARCHAR(512),
    failure_reason  TEXT,
    fraud_score     DOUBLE,
    correlation_id  VARCHAR,
    trace_id        VARCHAR,
    created_at      TIMESTAMP NOT NULL,      -- INDEX
    updated_at      TIMESTAMP
)

-- Transactional outbox (guarantees DB→Kafka consistency)
outbox_events (
    id              UUID PRIMARY KEY,
    event_type      VARCHAR NOT NULL,        -- Kafka topic name
    aggregate_id    VARCHAR NOT NULL,        -- paymentId (audit trail)
    partition_key   VARCHAR,                 -- userId (distribution key)
    payload         TEXT NOT NULL,           -- JSON event
    status          VARCHAR NOT NULL,        -- INDEX (NEW|PUBLISHED|FAILED)
    retry_count     INT NOT NULL DEFAULT 0,
    correlation_id  VARCHAR,
    trace_id        VARCHAR,
    created_at      TIMESTAMP NOT NULL,      -- INDEX
    published_at    TIMESTAMP,
    last_error      TEXT
)

-- Double-entry ledger
ledger_entries (
    id                UUID PRIMARY KEY,
    payment_id        UUID NOT NULL,         -- INDEX
    entry_type        VARCHAR NOT NULL,      -- DEBIT | CREDIT
    amount            DECIMAL(19,4) NOT NULL,
    currency          VARCHAR(3) NOT NULL,
    account_id        VARCHAR NOT NULL,      -- INDEX (userId for DEBIT, merchantId for CREDIT)
    counter_account_id VARCHAR NOT NULL,
    correlation_id    VARCHAR,
    created_at        TIMESTAMP NOT NULL     -- INDEX
)

-- Exactly-once consumer dedup
processed_events (
    event_id        VARCHAR PRIMARY KEY,     -- PK = dedup guard
    consumer_group  VARCHAR NOT NULL,
    topic           VARCHAR NOT NULL,
    processed_at    TIMESTAMP NOT NULL
)

-- Account balances with optimistic locking
accounts (
    id              UUID PRIMARY KEY,
    owner_id        VARCHAR NOT NULL,
    account_type    VARCHAR NOT NULL,        -- USER | MERCHANT
    balance         DECIMAL(19,4) NOT NULL,
    currency        VARCHAR(3) NOT NULL,
    version         BIGINT NOT NULL,         -- @Version (optimistic lock)
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP,
    UNIQUE (owner_id, currency)              -- one account per owner+currency
)

-- Webhook configuration
webhook_endpoints (id, merchant_id, url, events, secret, active)
webhook_deliveries (id, webhook_endpoint_id, payment_id, event_type, payload,
                    idempotency_key UNIQUE, status, attempt_count, last_attempted_at,
                    next_retry_at, last_response_code, last_error)
```

### Indexing Strategy

| Table | Index | Purpose |
|-------|-------|---------|
| `payments` | `idx_payments_user_id` | Fast lookup by user for list queries |
| `payments` | `idx_payments_status` | Filter by status (reconciliation, list queries) |
| `payments` | `idx_payments_created_at` | Time-range queries (reconciliation, pagination) |
| `payments` | `UNIQUE(idempotency_key)` | Idempotency enforcement — last-resort guard |
| `outbox_events` | `idx_outbox_status` | Publisher polls NEW events |
| `outbox_events` | `idx_outbox_created_at` | FIFO ordering for publishing |
| `ledger_entries` | `idx_ledger_payment_id` | Invariant verification (SUM by payment) |
| `ledger_entries` | `idx_ledger_account_id` | Account balance queries |
| `accounts` | `UNIQUE(owner_id, currency)` | One account per owner per currency |

### Consistency Model

| Data Type | Consistency | Mechanism |
|-----------|------------|-----------|
| **Payment state** | Strong (single-row) | `@Transactional`, `READ_COMMITTED` isolation |
| **Ledger entries** | Strong (within TX) | Written in same TX as payment status update; invariant checked before commit |
| **Outbox → Kafka** | Eventual (at-least-once) | Outbox poller retries; idempotent consumers on the receiving end |
| **Redis cache** | Eventual | Write-through on create; explicit eviction on status update; 30min TTL |
| **Account balance** | Strong (optimistic lock) | `@Version` field causes `OptimisticLockException` on concurrent updates; caller retries |
| **Cross-service** | Eventual | No 2PC; saga-like flow via Kafka events; reconciliation catches drift |

### Caching Strategy

| Cache Key | TTL | Purpose | Invalidation |
|-----------|-----|---------|-------------|
| `idem:<key>` | 24h | Skip DB lookup for idempotency replay | Write-through on create |
| `payment:<id>` | 30min | Skip DB read for payment polling | Explicit eviction after status change |
| `rate_limit:<userId>` | 60s | Sliding window counter for rate limiting | Auto-expire |
| `velocity:<userId>` | 60s | Fraud velocity counter | Auto-expire (Redis `INCR` + `EXPIRE`) |

**Fallback chain**: Redis → DB (for idempotency and payment reads); Redis → Caffeine in-memory (for rate limiting).

---

## 6. Key Features and Implementation Details

### 6.1 Transactional Outbox Pattern

**What**: Payment state and Kafka events are written in the same database transaction. A separate scheduler polls and publishes to Kafka.

**Why**: Eliminates the dual-write problem. Without outbox:
1. Save payment to DB ✅
2. Publish to Kafka ❌ (crash here → lost event, payment stuck in PENDING forever)

With outbox: both writes are atomic. If the TX commits, the event is guaranteed to eventually reach Kafka.

**Implementation**:
- `OutboxEvent` entity written via `OutboxPublisherService.createOutboxEvent()` inside caller's `@Transactional`
- Scheduler polls every 1s: `SELECT TOP 50 FROM outbox_events WHERE status='NEW' ORDER BY created_at`
- On Kafka send success → mark `PUBLISHED`; on failure → increment `retry_count`; after 3 failures → mark `FAILED`
- Failed events require manual intervention (reset status to NEW after fixing root cause)

**Edge Cases Handled**:
- Scheduler crash during publish → event stays `NEW`, republished on next poll → consumer dedup handles it
- Kafka ack lost → event republished → idempotent consumer skips duplicate
- Very high volume → batch size cap (50) prevents one poll from monopolizing the DB connection

### 6.2 Exactly-Once Processing

**Implementation** (three-layer defense):

| Layer | Mechanism | Scope |
|-------|-----------|-------|
| **Kafka Producer** | `enable.idempotence=true`, `acks=all`, `max.in.flight.requests=1` | Eliminates broker-side duplicates |
| **Processed Events Table** | `INSERT INTO processed_events` with PK constraint — first operation inside `@Transactional` | DB-level dedup, atomic with business logic |
| **Payment Status Guard** | `if (payment.getStatus() != PENDING) return` | Secondary safety net for crash between DB commit and Kafka ack |

**Critical detail**: `tryMarkProcessed()` uses `Propagation.MANDATORY` — it MUST run inside the caller's transaction. If the business logic fails and rolls back, the dedup row also rolls back, allowing correct reprocessing on retry.

### 6.3 Double-Entry Ledger

**Invariant**: For every payment, `SUM(DEBIT amounts) == SUM(CREDIT amounts)`.

**Flow**:
1. `LedgerService.recordPayment()` creates two `LedgerEntry` rows: DEBIT (user) + CREDIT (merchant)
2. `validateInvariant()` checks sums immediately after insert — throws `IllegalStateException` if violated
3. `AccountService.debit()` and `AccountService.credit()` update balances with `@Version` optimistic locking
4. `verifyLedgerBalance()` is called again at the `PaymentService` level as an explicit pre-condition before setting `SUCCESS`

**Edge Cases**:
- Insufficient balance → `InsufficientBalanceException` → payment marked `FAILED` (not rolled back entirely — `noRollbackFor` on service methods)
- Concurrent balance updates → `OptimisticLockException` → Spring Retry retries the entire transaction
- Duplicate call → `existsByPaymentId()` check returns early (idempotent)

### 6.4 Idempotency (Three-Tier)

```
Request with Idempotency-Key
  │
  ├── Layer 1: Redis cache (idem:<key> → paymentId)
  │     └── Hit? Return cached payment (0.5ms)
  │
  ├── Layer 2: DB SELECT ... FOR UPDATE (pessimistic lock)
  │     └── Hit? Return existing payment, backfill Redis cache
  │
  └── Layer 3: UNIQUE constraint on idempotency_key column
        └── Concurrent race? One INSERT wins, other gets constraint violation
```

**Why three layers**: Redis handles 99%+ of replays at sub-millisecond cost. DB lock handles Redis misses. UNIQUE constraint is the last-resort guard against concurrent identical requests that both miss Redis.

### 6.5 Adaptive Rate Limiting

**Primary (Redis)**: Lua script atomically increments counter and sets TTL. 100 requests/minute/user.

**Adaptive backpressure**: Rate limit dynamically adjusts based on Kafka consumer lag:
- Lag < 1,000 → full throughput (1.0× multiplier)
- Lag ≥ 5,000 → 50% reduction (0.5× multiplier)
- Lag ≥ 10,000 → 90% reduction (0.1× multiplier)

**Fallback (Caffeine)**: If Redis is down, per-JVM in-memory sliding window. Bounded at 50K entries with LRU eviction to prevent heap exhaustion. Trade-off: cluster-wide limit becomes per-pod.

### 6.6 Real-Time Fraud Detection

**Two-stage pipeline**:

| Stage | Check | Latency | Action on Trigger |
|-------|-------|---------|-------------------|
| Rule Engine | Currency not in allowlist | ~0μs | Reject immediately (score=0.95) |
| Rule Engine | Amount > $10,000 | ~0μs | Reject immediately (score=0.95) |
| Rule Engine | Velocity > 10 tx/60s | ~1ms | Reject immediately (score=0.95) |
| ML Model | IsolationForest anomaly score > threshold | ~5ms | Score + reason returned |

**ML Model Versioning**:
- v1: 3 features (amount, is_high_value, currency_risk) — backward compatible default
- v2: 11 features (adds velocity, behavioral, risk features from FeatureStore)
- Hot-reload via `/admin/model/upgrade` endpoint (protected by `X-Admin-Key`)
- Thread-safe swap: new model trained → atomic reference swap → old model GC'd

**Feature Store (Redis-backed)**:
- Velocity: sorted sets with timestamp scores, windowed counts (1min, 5min, 1hr)
- Behavioral: rolling averages, amount deviation, time since last transaction
- Risk: cached user/merchant fraud rates from batch pipeline
- Graceful degradation: falls back to basic 3-feature vector if Redis unavailable

### 6.7 Webhook Delivery

- Merchants register endpoints with event type subscriptions
- On `payment.processed`/`payment.failed`, `WebhookService` finds matching endpoints
- Delivery is `@Async` to avoid blocking Kafka consumer thread
- Exponential backoff retry: 5s → 30s → 120s → 600s → 1800s
- After 5 attempts → moved to DLQ status for manual review
- Each delivery has an idempotency key (`endpointId:paymentId:eventType`)

### 6.8 Reconciliation Service

Scheduled every 30 minutes. Detects 5 types of anomalies:

| Check | Condition | Severity |
|-------|-----------|----------|
| Stale payments | PENDING for > 10 minutes | WARNING |
| Ghost payments | SUCCESS with no ledger entries | CRITICAL |
| Duplicate ledger | > 2 entries per payment | CRITICAL |
| Ledger imbalance | SUM(DEBIT) ≠ SUM(CREDIT) | CRITICAL |
| Orphan ledger | Ledger rows with no matching payment | WARNING |

Alerts published to `reconciliation.alerts` Kafka topic for monitoring integration.

---

## 7. Scalability and Performance

### How the System Scales

| Tier | Scaling Strategy |
|------|-----------------|
| **API Gateway** | Horizontal — stateless, Netty event loop, shared Redis rate limit state |
| **Payment Service** | Horizontal — stateless, Kafka consumer group auto-rebalances partitions |
| **Fraud Service** | Horizontal — stateless, Kafka consumer group; model loaded per-instance |
| **PostgreSQL** | Vertical (write master) + horizontal (read replicas for GET queries) |
| **Kafka** | Horizontal — add partitions + consumers; increase replication factor |
| **Redis** | Vertical (single instance) → Redis Cluster for horizontal scaling |

### Bottlenecks and Optimizations

| Bottleneck | Detection | Mitigation |
|------------|----------|------------|
| DB connection pool exhaustion | `hikaricp_connections_pending > 0` | Increase pool size, optimize queries, add read replicas |
| Kafka consumer lag | `kafka_consumer_lag_total` increasing | Increase concurrency, add partitions, optimize consumer processing |
| Hot Kafka partitions | Uneven `kafka_consumer_lag_max_partition` | Composite partition key with salt factor |
| Redis saturation | Rate limit fallback logs, Lettuce timeouts | Redis Cluster, increase pool, tune `maxmemory-policy` |
| Payment table growth | Slow `SELECT` queries | Archival strategy (move old payments to cold storage) |

### Throughput and Latency

**Synchronous path** (POST /payments):
```
Redis Lua rate limit:    ~1ms
Redis GET idempotency:   ~0.5ms
PG INSERT payment:       ~3ms
PG INSERT outbox_event:  ~2ms
Redis SET cache:         ~0.5ms
─────────────────────────────
Total:                   ~10ms (p50), ~50ms (p95 under load)
```

**Asynchronous path** (payment.created → SUCCESS):
```
Outbox poll + Kafka publish:  ~1s (poll interval) + 5ms
PaymentEventConsumer:         ~2ms
Fraud Service scoring:        ~6ms (rule + ML)
FraudResultConsumer:          ~15ms (dedup + ledger + account + save)
─────────────────────────────
Total end-to-end:             ~1.0-1.5s
```

### Load Test Profiles

| Profile | VUs | Duration | Purpose |
|---------|-----|----------|---------|
| Default | 10→100→10 | 2.5min | Smoke test |
| Stress | 100→1500 | 5.5min | 1,000+ TPS, find breaking point |
| Soak | 200 sustained | 34min | Memory leak detection |
| Spike | 10→2000→10 | 1.5min | Backpressure validation |

**k6 thresholds**: p95 < 500ms, p99 < 1000ms, error rate < 5%, idempotency replay success > 99%.

---

## 8. Reliability and Fault Tolerance

### Retry Mechanisms

| Component | Retry Strategy | Config |
|-----------|---------------|--------|
| `processPaymentResult()` | `@Retryable`: 3 attempts, 200ms × 2 exponential backoff, max 2s | For `TransientDataAccessException` only |
| Outbox Publisher | 3 retries per event, then mark FAILED | `app.outbox.max-retries: 3` |
| Kafka Producer | 5 retries, 120s delivery timeout | `retries: 5`, `delivery.timeout.ms: 120000` |
| Kafka Consumer (framework) | `ExponentialBackOff`: 1s initial, 2× multiplier, 30s max | Configured in `KafkaConfig` |
| API Gateway | 3 retries on 502/503/504 for GET only, 100ms→500ms | Spring Cloud Gateway Retry filter |
| Webhook Delivery | 5 attempts, backoff: 5s→30s→120s→600s→1800s | Then moved to DLQ status |

### Circuit Breakers

| Circuit Breaker | Config | Fallback |
|-----------------|--------|----------|
| Fraud Service (Resilience4j) | 10-call sliding window, 50% failure threshold, 10s open, 3 half-open probes | Allow payment through (fail-open), score=0.5, `fallback=true` flag |
| Payment Service (Gateway) | Spring Cloud Gateway CircuitBreaker filter | `FallbackController` returns 503 with "Service temporarily unavailable" |

**Why fail-open for fraud**: Blocking legitimate payments is worse than letting a suspicious one through for async review. The neutral score (0.5) flags it for manual inspection.

### Idempotency

| Layer | Mechanism | Coverage |
|-------|-----------|----------|
| HTTP API | `Idempotency-Key` header → Redis cache → DB `SELECT FOR UPDATE` → `UNIQUE` constraint | Payment creation |
| Kafka consumers | `processed_events` table with PK on `event_id` | Event processing |
| Ledger writes | `existsByPaymentId()` check | Prevents double ledger entries |
| Webhook delivery | `idempotency_key` column (endpointId:paymentId:eventType) | Prevents duplicate deliveries |

### Exactly-Once vs At-Least-Once

The system achieves **effectively-once** semantics:
- **Kafka producer**: idempotent (`enable.idempotence=true`) — eliminates broker-side duplicates
- **Kafka delivery**: at-least-once (manual ack after processing)
- **Consumer dedup**: `processed_events` PK insert inside `@Transactional` — atomic with business logic
- **Net result**: at-least-once delivery + idempotent consumers = effectively-once processing

### DLQ (Dead Letter Queue) Strategy

Every topic has a `.DLQ` counterpart: `payment.created.DLQ`, `fraud.result.DLQ`, etc. After all retry attempts are exhausted, events are forwarded to DLQ for manual investigation. The DLQ topics have 1 partition (ordering doesn't matter for manual review).

---

## 9. Security Considerations

### Authentication / Authorization

| Layer | Mechanism | Detail |
|-------|-----------|--------|
| API Gateway | JWT Bearer (HS256) | Symmetric key, local validation, no auth server roundtrip |
| Downstream services | `X-User-Id` header (injected by gateway) | Trust only gateway-injected headers; raw `Authorization` stripped |
| Fraud admin endpoints | `X-Admin-Key` header | Simple shared secret for `/admin/model/*` |
| Webhook signatures | SHA-256 HMAC | Each webhook endpoint has a unique secret |

### Data Validation

| Validation | Location | Mechanism |
|------------|----------|-----------|
| Request body | `CreatePaymentRequest` | `@NotNull`, `@DecimalMin("0.01")`, `@Pattern("^[A-Z]{3}$")` |
| Content-Type | API Gateway (`RequestValidationFilter`) | Rejects non-JSON POST requests (415) |
| Idempotency-Key | API Gateway (`RequestValidationFilter`) | Rejects POST /payments without header (400) |
| Body size | API Gateway (`RequestValidationFilter`) | 1MB max (413) |
| Webhook URL | `RegisterWebhookRequest` | `@Pattern("^https://...")` — HTTPS only, blocks SSRF |
| Currency | Fraud Service | Allowlist of 7 currencies; unknown → auto-reject |

### Threat Modeling

| Threat | Mitigation |
|--------|-----------|
| **Replay attack** | Idempotency-Key ensures duplicate requests are safe |
| **Rate abuse** | Redis + Caffeine rate limiting per userId; adaptive backpressure |
| **IP spoofing** | Rate limiter uses `remoteAddress` directly, NOT `X-Forwarded-For` |
| **SSRF via webhook** | Webhook URLs validated with `^https://` regex pattern |
| **JWT secret leak** | Secret via environment variable; production uses rotation |
| **SQL injection** | JPA parameterized queries throughout; no raw SQL concatenation |
| **Unauthorized admin access** | Fraud admin endpoints require `X-Admin-Key` header |

---

## 10. Observability

### Logging Strategy

- **Structured JSON logging** via Logback (`logback-spring.xml`)
- **MDC (Mapped Diagnostic Context)** propagation: `traceId`, `correlationId`, `paymentId`, `eventId`
- **CorrelationIdFilter** (servlet filter) sets MDC on every HTTP request
- **Kafka consumers** set MDC from message headers before processing
- Log levels: `INFO` for business events, `WARN` for degraded-mode fallbacks, `ERROR` for failures

**Key log pattern**: Every log line includes `traceId` + `correlationId` → enables full distributed trace reconstruction: HTTP request → Kafka events → fraud service → final status.

### Metrics (Prometheus)

| Metric | Type | Tags | Purpose |
|--------|------|------|---------|
| `payments_created_total` | Counter | currency | Payment creation throughput |
| `payments_processed_total` | Counter | currency, result | SUCCESS/FAILED/FRAUD_REJECTED counts |
| `fraud_detected_total` | Counter | reason | Fraud detection rate by reason |
| `payment_processing_seconds` | Timer | status | End-to-end processing latency |
| `active_payments_pending` | Gauge | — | Current PENDING payment count |
| `kafka_consumer_lag_total` | Gauge | consumer_group | Backpressure signal |
| `kafka_consumer_lag_max_partition` | Gauge | consumer_group | Hot partition detection |
| `outbox_events_published_total` | Counter | — | Outbox throughput |
| `outbox_events_failed_total` | Counter | — | Outbox failure rate |
| `fraud_requests_total` | Counter | result, stage | Fraud service throughput |
| `fraud_request_duration_seconds` | Histogram | — | Fraud scoring latency distribution |
| `fraud_score_distribution` | Histogram | — | ML score distribution |

### Monitoring and Alerting

- **Prometheus**: scrapes all 3 services every 15 seconds
- **Grafana**: auto-provisioned 10-panel dashboard via JSON config:
  - Payment throughput, processing latency, Kafka consumer lag, fraud detection rates, error rates
- **Reconciliation alerts**: published to `reconciliation.alerts` Kafka topic every 30 minutes
- **Backpressure**: logged at WARN (1K lag), ERROR (10K lag) levels

### Tracing

- Distributed trace via `x-trace-id` header — generated once at API Gateway entry point
- Propagated through: HTTP headers → Kafka message headers → MDC in every service
- Reconstructable across: API Gateway → Payment Service → Kafka → Fraud Service → Kafka → Payment Service → Webhook

---

## 11. Deployment and DevOps

### Docker / Containerization

All services are containerized with multi-stage Dockerfiles:

```yaml
services:
  postgres:       image: postgres:15                 # port 5433
  zookeeper:      image: confluentinc/cp-zookeeper:7.5.0
  kafka:          image: confluentinc/cp-kafka:7.5.0 # internal 9092, external 29092
  redis:          image: redis:7-alpine              # port 6379, AOF persistence
  fraud-service:  build: ./fraud-service             # port 8000
  payment-service: build: ./payment-service          # port 8080
  api-gateway:    build: ./api-gateway               # port 8090
  prometheus:     image: prom/prometheus:v2.47.0      # port 9090
  grafana:        image: grafana/grafana:10.1.0      # port 3000
```

### Environment Configuration

| Variable | Service | Purpose |
|----------|---------|---------|
| `SPRING_DATASOURCE_URL` | payment-service | DB connection (jdbc:postgresql://...) |
| `SPRING_KAFKA_BOOTSTRAP_SERVERS` | payment-service | Kafka broker address |
| `SPRING_DATA_REDIS_HOST` | payment-service | Redis host |
| `FRAUD_SERVICE_URL` | payment-service | Fraud service base URL |
| `GATEWAY_JWT_SECRET` | api-gateway | HS256 signing key (min 32 chars) |
| `REDIS_URL` | fraud-service | Redis for velocity tracking |
| `KAFKA_BOOTSTRAP_SERVERS` | fraud-service | Kafka broker address |

### Local Development vs Production

| Concern | Local | Production |
|---------|-------|-----------|
| Database | PostgreSQL in Docker (port 5433) | Managed PostgreSQL (RDS/Cloud SQL) with replicas |
| Kafka | Single broker, 3 partitions, replication=1 | Multi-broker cluster, replication=3 |
| Redis | Single instance, AOF, LRU 256MB | Redis Cluster, Sentinel for HA |
| JWT Secret | Default dev value | Rotated via secrets manager |
| Monitoring | Prometheus + Grafana in Docker | Managed monitoring (Datadog, CloudWatch) |
| Load testing | k6 against localhost | k6 against staging environment |
| `ddl-auto` | `update` (auto-migrate) | `validate` (migration tool like Flyway) |

### Failure Simulation Scripts

Located in `scripts/failure-simulation/`:
- `database-failure.sh` — Pause/unpause PostgreSQL container
- `kafka-failure.sh` — Stop/start Kafka broker
- `redis-failure.sh` — Stop/start Redis
- `fraud-service-failure.sh` — Stop/start fraud service
- `network-partition.sh` — Simulate network partition between services

---

## 12. Trade-offs and Design Decisions

### Decisions Made

| Decision | Why | Trade-off |
|----------|-----|-----------|
| **Outbox polling (1s) vs CDC** | Simpler, no Debezium/connector infrastructure | 1s latency floor; DB polling load |
| **userId as partition key (not paymentId)** | Enables per-user ordering, velocity checks, stateful stream processing | Hot partitions for high-volume users (mitigated by salt) |
| **Fail-open fraud circuit breaker** | Legitimate payments shouldn't be blocked by infra failures | Some fraudulent payments may slip through during outage |
| **HS256 JWT (symmetric) vs RS256** | Simpler; single service issues tokens | Secret must be shared across services; no public key verification |
| **Caffeine fallback for rate limiting** | Rate limiting should never fail-open (fraud exposure) | Per-pod limits during Redis outage (not cluster-wide) |
| **Manual Kafka ack** | Enables exactly-once with DB dedup | Requires careful error handling; risk of message loss if ack before processing |
| **BigDecimal for amounts** | Prevents floating-point rounding errors in financial math | More verbose than `double`; requires explicit scale management |
| **NoRollbackFor InsufficientBalance** | Payment should be marked FAILED, not left in PENDING | Requires careful understanding of Spring TX rollback semantics |
| **Optimistic locking (accounts)** | Low contention in normal operation; no lock waits | Retries under high concurrency for same account; slightly higher p99 |
| **In-process fraud velocity fallback** | Fraud service must not fail-close when Redis is down | Velocity tracking is per-process, not cross-instance |

### What Would You Improve with More Time

| Improvement | Why | Effort |
|-------------|-----|--------|
| **Debezium CDC** instead of outbox polling | Eliminates polling overhead; sub-100ms event publish latency | Medium — adds Kafka Connect infrastructure |
| **Flyway/Liquibase** for schema migration | `ddl-auto: update` is unsafe in production | Low — migration tool + version control for DDL |
| **Saga orchestrator** for multi-step flows | Current flow relies on event choreography; hard to add compensation logic | High — requires saga state machine |
| **Read replicas** for GET /payments | Separates read/write load on PostgreSQL | Low — Spring `@Transactional(readOnly=true)` routing |
| **RS256 (asymmetric) JWT** | Removes shared secret between gateway and token issuer | Low — switch to JWKS endpoint |
| **OpenTelemetry** instead of custom tracing | Industry standard for distributed tracing; auto-instrumentation | Medium — replace MDC-based approach with OTel SDK |
| **Schema Registry** for Kafka | Enforce schema evolution, prevent breaking changes | Medium — add Confluent Schema Registry |
| **Account sharding** | Single-master PG is a bottleneck at extreme scale | High — requires sharding key strategy and distributed transactions |
| **Blue/green deployment** | Zero-downtime releases with Kafka consumer group rebalancing | Medium — requires orchestration tooling |

---

## 13. Interview Questions and Answers

### Basic (Project Explanation, Tech Choices)

**Q: Walk me through the architecture of your payments system.**

A: It's an event-driven microservices system with three services: a Spring Boot Payment Service that handles the full payment lifecycle, a Python FastAPI Fraud Service that runs rule-based and ML-based fraud detection, and a Spring Cloud Gateway for authentication and rate limiting. PostgreSQL is the source of truth, Kafka handles async event streaming via the Transactional Outbox Pattern, and Redis provides caching, rate limiting, and pub/sub. The key design principle is that the synchronous write path (client → API → DB) is fast and idempotent, while all downstream processing (fraud, ledger, webhooks) is asynchronous and exactly-once-safe.

**Q: Why did you choose Kafka over RabbitMQ?**

A: Three reasons. First, Kafka provides durable, ordered logs per partition — critical for payment events where order matters for per-user velocity tracking. Second, Kafka's consumer group model lets us scale consumers horizontally by just adding instances. Third, the 7-day retention enables event replay for debugging and reprocessing without needing to re-emit events. RabbitMQ is simpler but doesn't provide these guarantees — it's a message broker, not an event log.

**Q: Why is the Fraud Service in Python instead of Java?**

A: The Fraud Service uses scikit-learn for IsolationForest ML inference and numpy for feature engineering. Python's ML ecosystem is vastly more mature than Java's — model iteration is faster, and the data science team (if there were one) would naturally work in Python. The cost is a separate runtime, but the services are fully decoupled via Kafka, so the language boundary is clean.

---

### Intermediate (Design Decisions, Trade-offs)

**Q: Explain the Transactional Outbox Pattern and why you used it.**

A: The dual-write problem is: if I save a payment to the DB and then publish to Kafka, a crash between those two steps means the event is lost. The outbox pattern solves this by writing the Kafka event as a row in an `outbox_events` table within the same DB transaction as the payment. A separate scheduler polls this table every second and publishes to Kafka. If the scheduler crashes, it simply retries on the next poll — the event is already safely in the DB. The trade-off is a 1-second latency floor and DB polling overhead — a production system could use Debezium CDC to eliminate this.

**Q: How do you achieve exactly-once processing?**

A: It's a three-layer defense. Layer 1: the Kafka producer is idempotent (`enable.idempotence=true`, `acks=all`, `max.in.flight=1`), which eliminates broker-side duplicates. Layer 2: each consumer's first operation inside its `@Transactional` method is an INSERT into `processed_events` with the event ID as the primary key. If the event was already processed, the PK constraint fails and we skip. Layer 3: a payment status guard — if the payment is already `SUCCESS`, we return early. The critical insight is that the `processed_events` insert and the business logic are in the same transaction, so they commit or roll back together.

**Q: Why optimistic locking for account balances instead of pessimistic?**

A: Under normal load, concurrent updates to the same account are rare — most payments involve different users. Optimistic locking with `@Version` avoids holding DB locks during the entire transaction, which would significantly reduce throughput. On conflict, Spring Retry retries the entire operation. In a system where the same account is updated thousands of times per second (e.g., a clearinghouse account), pessimistic locking or database-level `SELECT ... FOR UPDATE SKIP LOCKED` would be a better choice.

**Q: How does your idempotency implementation handle concurrent duplicate requests?**

A: Three tiers. First, Redis: `GET idem:<key>` — sub-millisecond, handles 99%+ of replays. Second, if Redis misses, `SELECT ... FOR UPDATE` on the `idempotency_key` column — the pessimistic lock serializes concurrent requests to the same key, so one wins and the other waits and then sees the existing record. Third, if both requests somehow bypass the first two layers, the `UNIQUE` constraint on `idempotency_key` column catches it at the DB level. The three tiers trade extra infra (Redis) for performance — without Redis, every idempotency check would hit the DB.

**Q: Explain your rate limiting design and how it handles Redis failures.**

A: The primary rate limiter is a Redis Lua script that atomically increments a counter and sets a TTL — classic sliding window pattern. If Redis is unavailable (connection failure or timeout), it falls back to a Caffeine in-memory cache bounded at 50K entries with LRU eviction. The bound is critical — without it, an attacker sending millions of unique user IDs could cause unbounded heap growth. The trade-off during Redis outage is that the effective cluster-wide limit becomes `limit × pod_count` since each pod enforces its own limit independently.

---

### Advanced (Scaling, Failure Scenarios, Optimizations)

**Q: How would you scale this system to handle 100K TPS?**

A: The bottleneck shifts depending on the tier. For the **API layer**: it's stateless, so horizontal scaling behind a load balancer works. For **Kafka**: increase partitions (e.g., 30-50) and add consumer instances to match. For **PostgreSQL** (the real bottleneck): first, add read replicas for GET queries and route read-only transactions there. For writes, consider sharding by `user_id` using Citus or Vitess. The outbox table would need partitioning or replacement with Debezium CDC. For **Redis**: move to Redis Cluster with multiple shards. For **fraud scoring**: the ML model is stateless, so horizontal scaling is straightforward; the feature store just needs a Redis Cluster backend. The fundamental change at 100K TPS is moving from a single-master PostgreSQL to a sharded architecture.

**Q: What happens if the fraud service is down for 30 minutes?**

A: Payments continue to be created (status=PENDING) and `payment.created` events accumulate in Kafka. Since Kafka has 7-day retention, no events are lost. When the fraud service recovers, its consumer group resumes from the last committed offset, processing all queued events. The backpressure system detects the growing Kafka lag and reduces the rate limit to slow incoming payments. The reconciliation service flags payments stuck in PENDING for >10 minutes. If the HTTP fallback path is used (circuit breaker), payments are allowed through with `score=0.5` and `fallback=true` for async review.

**Q: Describe a cascading failure scenario and how your system prevents it.**

A: Scenario: PostgreSQL becomes slow (disk I/O spike). The HikariCP connection pool fills up → Kafka consumer processing slows → consumer lag increases → BackpressureService detects lag >5,000 → rate limit multiplier drops to 0.5× → client-facing API starts returning 429s at a lower threshold. This prevents more payments from entering the system while the bottleneck resolves. Without backpressure, the payment creation rate would stay constant, the outbox table would grow, Kafka lag would spiral, and eventually the whole system would collapse. The key insight is **feedback loop**: the consumer lag metric (output) controls the rate limit (input).

**Q: How do you handle hot partitions in Kafka?**

A: If userId is the partition key, a high-volume user (e.g., a large merchant) sends all their events to one partition, creating a hot spot. I mitigate this with a composite partition key: `userId:hash(paymentId) % saltFactor`. With `saltFactor=3`, a single user's events are distributed across 3 partitions instead of 1. The trade-off is relaxed per-user ordering — events for the same user may span 3 partitions instead of being strictly ordered on one. For this system, that's acceptable because fraud velocity tracking uses Redis (cross-partition), and the ledger dedup is based on `paymentId` (unique), not user ordering.

**Q: If you had to redesign the inter-service communication, what would you change?**

A: I'd consider three things. First, replace the outbox polling with **Debezium CDC** — it tails the PostgreSQL WAL and publishes changes to Kafka with sub-100ms latency, eliminating the 1-second polling floor. Second, add a **Schema Registry** (Confluent or Apicurio) to enforce Avro/Protobuf schemas with backward compatibility checks — right now, any producer can publish malformed JSON. Third, for the fraud pipeline specifically, I'd evaluate **Kafka Streams** for stateful stream processing — maintaining velocity counters natively in Kafka's state stores rather than relying on Redis, which would eliminate a dependency.

---

### Scenario-Based

**Q: What happens if Kafka goes down?**

A: Payment creation continues to work — the payment is saved to PostgreSQL and the outbox event is written in the same transaction. The OutboxPublisherService keeps polling every second, but sends fail and increments retry counts. After 3 failures, events are marked FAILED. When Kafka recovers, events that weren't exhausted resume publishing. FAILED events require manual reset (`UPDATE outbox_events SET status='NEW', retry_count=0`). On the consumer side, no new events arrive, so no processing happens — but no data is lost. After recovery, consumers resume from last committed offset.

**Q: What if the same payment is processed twice?**

A: The system has three guards. First, the `processed_events` table: the consumer inserts a row with the event ID as PK. The second attempt hits a `DataIntegrityViolationException` and the method returns false (skip). Second, the payment status guard: if `status != PENDING`, the consumer returns early. Third, the ledger has its own `existsByPaymentId()` check. All three operate inside the same `@Transactional`, so either all side effects commit or none do. In practice, the PK dedup catches >99% of duplicates; the status guard handles the edge case of a crash between DB commit and Kafka ack.

**Q: What happens if Redis goes down during a production peak?**

A: Each Redis-dependent feature has a fallback: Rate limiting switches to Caffeine (50K entries, 2min TTL, LRU eviction). Payment cache misses fall through to PostgreSQL queries. Idempotency checks fall through to `SELECT ... FOR UPDATE` on the DB. Fraud velocity tracking in the Python service falls back to an in-process `deque`. WebSocket pub/sub fails silently (UI updates stop but payments continue). The system degrades but doesn't break. The key trade-offs: rate limiting becomes per-pod instead of cluster-wide, and cache miss rate increases DB load.

**Q: A customer reports a double charge. How do you investigate?**

A: Step 1: Query `ledger_entries` by the payment ID — each payment should have exactly 1 DEBIT + 1 CREDIT. If there are duplicates, the reconciliation service would have already flagged it. Step 2: Check `processed_events` for the event IDs — if the same event was processed twice, there's a dedup bug. Step 3: Check the idempotency layer — query `payments` by `idempotency_key` to see if two separate payments were created with different keys for the same logical operation (client bug). Step 4: Check the `outbox_events` for duplicate publish evidence. Step 5: Check Kafka consumer logs for rebalance events that might have caused duplicate delivery. The distributed trace (`x-trace-id`) ties all of these together.

---

## 14. Elevator Pitch

### Backend-Focused (60 seconds)

"I built a distributed payments processing platform using Java Spring Boot, Kafka, PostgreSQL, and Redis. The system handles the full payment lifecycle — from ingestion through fraud detection to ledger recording — using an event-driven architecture. The key engineering challenge was ensuring zero payment loss and exactly-once processing in a distributed system. I solved the dual-write problem with the Transactional Outbox Pattern, where payment state and Kafka events are written atomically in the same DB transaction. The system implements a double-entry ledger with invariant enforcement, three-tier idempotency (Redis → DB lock → UNIQUE constraint), and adaptive backpressure that throttles ingestion based on Kafka consumer lag. Every dependency — Redis, Kafka, the fraud service — has a fallback path, so the system degrades gracefully under partial failure."

### System Design-Focused (60 seconds)

"I designed and implemented a Stripe-like payment platform that demonstrates core distributed systems patterns. The architecture is event-driven: a synchronous write path gives clients immediate 201 responses, while fraud detection, ledger recording, and webhook delivery happen asynchronously via Kafka. The Transactional Outbox Pattern guarantees DB-to-Kafka consistency without two-phase commit. Exactly-once processing is achieved through idempotent Kafka producers plus a processed_events dedup table that commits atomically with business logic. The system includes a real-time fraud pipeline with rule-based and ML-based scoring, a double-entry ledger with balance verification, circuit breakers with fail-open policies, and a reconciliation service that detects stale payments and ledger imbalances. I validated the design under 1,500 virtual users with k6 and confirmed graceful degradation using failure simulation scripts."

### AI/ML-Focused (60 seconds)

"The fraud detection pipeline in my payments system uses a two-stage approach: deterministic rule checks (velocity limits, amount thresholds, currency allowlists) followed by an IsolationForest ML model for anomaly detection. The ML pipeline includes a Redis-backed Feature Store that computes real-time velocity features (1min/5min/1hr windows), behavioral features (rolling averages, amount deviation), and cached risk scores. The model supports versioning with hot-reload — v1 uses 3 features for backward compatibility, v2 uses 11 features from the full engineering pipeline. The model manager handles feature count mismatches gracefully: padding or truncating as needed. The entire fraud check runs in under 10ms, and the system falls back to basic features if Redis is unavailable, maintaining fraud detection even in degraded mode."

---

## 15. Resume Bullet Mapping

**Senior-Level Resume Bullets:**

- Designed and implemented an event-driven payment processing platform using Java 17/Spring Boot, Kafka, PostgreSQL, and Redis, achieving exactly-once processing semantics through the Transactional Outbox Pattern and idempotent consumer dedup
- Built a double-entry ledger system with atomic invariant enforcement (`SUM(DEBIT)==SUM(CREDIT)`) and optimistic locking on account balances, ensuring zero-discrepancy financial record-keeping
- Developed a real-time ML fraud detection pipeline (Python/FastAPI + IsolationForest) with two-stage scoring (rule engine + ML), Redis-backed feature engineering, and hot-reloadable model versioning
- Engineered a three-tier idempotency system (Redis cache → pessimistic DB lock → UNIQUE constraint) handling 1,000+ TPS with p95 latency < 500ms, validated via k6 stress tests at 1,500 concurrent users
- Implemented adaptive backpressure that dynamically throttles API ingestion based on Kafka consumer lag, preventing cascading failures during downstream bottlenecks
- Built an API Gateway (Spring Cloud Gateway) with JWT authentication, Redis token-bucket rate limiting, circuit breaker patterns, and request enrichment with distributed tracing headers
- Designed graceful degradation for all external dependencies — Redis failures fall back to bounded Caffeine caches, fraud service outages trigger fail-open circuit breakers, and Kafka downtime is absorbed by the outbox table
- Created a scheduled reconciliation service detecting 5 anomaly types (stale payments, ghost successes, ledger imbalances, duplicate entries, orphan records) with Kafka-based alerting
- Implemented end-to-end distributed tracing via `x-trace-id` header propagation across HTTP, Kafka, and inter-service boundaries with structured JSON logging and Prometheus/Grafana observability
