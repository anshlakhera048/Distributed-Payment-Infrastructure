# Distributed Payments System

A production-grade distributed payments platform demonstrating event-driven microservices, the Transactional Outbox Pattern, real-time ML fraud detection, and exactly-once processing semantics — all running on Docker Compose.

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Architecture Diagram](#2-architecture-diagram)
3. [Services Description](#3-services-description)
4. [Setup Instructions](#4-setup-instructions)
5. [Running the System](#5-running-the-system)
6. [Database Access](#6-database-access)
7. [Complete Testing Guide](#7-complete-testing-guide)
8. [Failure Testing Guide](#8-failure-testing-guide)
9. [Load Testing Guide](#9-load-testing-guide)
10. [Observability](#10-observability)
11. [Troubleshooting](#11-troubleshooting)
12. [Frontend Dashboard](#12-frontend-dashboard)

---

## 1. System Overview

This system implements a complete payment processing pipeline:

- **Payment Service** (Java/Spring Boot): accepts payments, enforces idempotency, writes to PostgreSQL, publishes events via outbox pattern
- **Fraud Service** (Python/FastAPI): scores transactions using rule engine + IsolationForest ML model, communicates via Kafka
- **API Gateway** (Spring Cloud Gateway): JWT authentication, Redis-backed rate limiting, circuit breakers
- **Kafka**: event streaming backbone (payment.created → fraud.request → fraud.result → payment.processed)
- **PostgreSQL**: durable storage for payments, ledger entries, outbox events, processed events, webhooks
- **Redis**: caching (idempotency keys, payment responses), rate limiting, WebSocket pub/sub

### Key Design Patterns

| Pattern | Implementation |
|---------|---------------|
| Transactional Outbox | `OutboxEvent` table + `OutboxPublisherService` polls every 1s |
| Exactly-Once Consumers | `ProcessedEvent` table + `IdempotentConsumerService` (PK constraint dedup) |
| Double-Entry Ledger | `LedgerEntry` — DEBIT + CREDIT per payment, invariant enforced atomically |
| Circuit Breaker | Resilience4j on fraud-service HTTP client (fail-open fallback) |
| Idempotent API | `Idempotency-Key` header → Redis cache → DB unique constraint |
| Rate Limiting | Redis Lua script + Caffeine in-memory fallback |
| DLQ Routing | Every consumer catches unrecoverable errors → sends to `*.DLQ` topic |
| Distributed Tracing | `traceId` + `correlationId` propagated via HTTP headers → MDC → Kafka headers |

---

## 2. Architecture Diagram

```
                              ┌─────────────────────────────────────────┐
                              │     Client / k6 Load Test               │
                              └──────────────┬──────────────────────────┘
                                             │ JWT + Idempotency-Key
                                             ▼
                              ┌─────────────────────────────────────────┐
                              │    api-gateway  (Spring Cloud · :8090)  │
                              │    JWT auth · Redis rate limit · CB     │
                              └──────────────┬──────────────────────────┘
                                             │
                                             ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                    payment-service  (Spring Boot 3.4.4 · :8080)                  │
│                                                                                  │
│  POST /payments → PaymentService.createPayment()                                 │
│     ├── Rate limit check (Redis → Caffeine fallback)                             │
│     ├── Idempotency check (Redis cache → DB pessimistic lock)                    │
│     ├── Save Payment (status=PENDING) + OutboxEvent → same DB transaction        │
│     └── Cache idempotency key in Redis                                           │
│                                                                                  │
│  OutboxPublisherService (poll every 1s)                                          │
│     └── Publish payment.created → Kafka (partition key=userId)                   │
│         Headers: x-event-id, x-correlation-id, x-trace-id                        │
│                                                                                  │
│  PaymentEventConsumer ← payment.created                                          │
│     └── Forward → fraud.request (propagate all trace headers)                    │
│                                                                                  │
│  FraudResultConsumer ← fraud.result                                              │
│     └── processPaymentResult() [@Transactional]                                  │
│         ├── IdempotentConsumerService.tryMarkProcessed() [dedup]                 │
│         ├── LedgerService.recordPayment() [DEBIT + CREDIT]                       │
│         ├── LedgerService.verifyLedgerBalance() [invariant check]                │
│         ├── Payment status → SUCCESS / FRAUD_REJECTED                            │
│         └── OutboxEvent → payment.processed / payment.failed                     │
│                                                                                  │
│  PaymentProcessedConsumer ← payment.processed / payment.failed                   │
│     ├── PaymentNotificationService.dispatchNotification() [@Transactional]       │
│     │   ├── IdempotentConsumerService.tryMarkProcessed() [dedup]                 │
│     │   └── WebhookService.dispatchEvent() [webhook delivery]                    │
│     └── WebSocketBroadcaster.broadcast() [fire-and-forget, outside TX]           │
│                                                                                  │
│  ReconciliationService (@Scheduled cron)                                         │
│     └── Detects: stale PENDING, ghost SUCCESS, duplicate ledger, imbalance       │
│                                                                                  │
│  Redis :6379 — rate limiting · idempotency cache · payment cache · WS pub/sub    │
│  PostgreSQL :5433 — payments · outbox_events · processed_events · ledger_entries │
└──────────────────────────────────────────────────────────────────────────────────┘
                         │ Kafka: fraud.request
                         ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                    fraud-service  (FastAPI · Python 3.11 · :8000)                │
│                                                                                  │
│  Stage 1 — Rule engine: velocity (>10 tx/60s), amount >$10K, currency allowlist  │
│  Stage 2 — ML scoring: IsolationForest [amount, is_high_value, currency_risk]    │
│  3 parallel Kafka consumer workers · x-trace-id header forwarded                 │
│  Prometheus metrics at /metrics · /health for Docker healthcheck                 │
└──────────────────────────────────────────────────────────────────────────────────┘
                         │ Kafka: fraud.result
                         └──► FraudResultConsumer (above)
```

---

## 3. Services Description

### Payment Service (Java 17 / Spring Boot 3.4.4)

| Component | Class | Responsibility |
|-----------|-------|---------------|
| HTTP API | `PaymentController` | POST /payments, GET /payments/:id |
| Business Logic | `PaymentService` | Create payment, process fraud result, idempotency |
| Outbox Pattern | `OutboxPublisherService` | Poll outbox_events table, publish to Kafka |
| Ledger | `LedgerService` | Double-entry bookkeeping, invariant verification |
| Deduplication | `IdempotentConsumerService` | Insert into processed_events (PK constraint) |
| Rate Limiting | `RateLimiterService` | Redis Lua script + Caffeine fallback |
| Caching | `PaymentCacheService` | Redis cache for idempotency keys + payment responses |
| Kafka Consumer | `PaymentEventConsumer` | payment.created → fraud.request |
| Kafka Consumer | `FraudResultConsumer` | fraud.result → processPaymentResult() |
| Kafka Consumer | `PaymentProcessedConsumer` | payment.processed → notifications |
| Notifications | `PaymentNotificationService` | Webhook dispatch (separate bean for AOP proxy) |
| Webhooks | `WebhookService` | HTTP delivery with exponential backoff retry |
| Reconciliation | `ReconciliationService` | Cron job: detect stale/ghost/duplicate/orphan entries |
| Fraud Client | `FraudServiceClient` | HTTP fallback path with circuit breaker |
| Tracing | `CorrelationIdFilter` | Generates traceId + correlationId → MDC |
| Metrics | `PaymentMetrics` | Prometheus counters + timers |
| Error Handling | `GlobalExceptionHandler` | Maps exceptions to standard error response |

### Fraud Service (Python 3.11 / FastAPI)

| Component | Responsibility |
|-----------|---------------|
| `/score` endpoint | Synchronous fraud scoring (HTTP) |
| Kafka consumer loop | Async fraud.request → fraud.result pipeline |
| Rule engine | Velocity check, amount threshold, currency allowlist |
| ML model | IsolationForest trained on synthetic data at startup |
| `/health` | Docker healthcheck endpoint |
| `/metrics` | Prometheus metrics (fraud_requests_total, etc.) |

### API Gateway (Spring Cloud Gateway 2024.0.1)

| Component | Responsibility |
|-----------|---------------|
| JWT validation | HS256 symmetric key (`GATEWAY_JWT_SECRET`) |
| Rate limiting | Redis-backed token bucket (100 rps, burst 200) |
| Request enrichment | Injects X-Correlation-ID, X-User-Id headers |
| Circuit breaker | Opens on downstream failures, fallback endpoint |
| Retry filter | 3 retries on 502/503/504 for GET requests |

---

## 4. Setup Instructions

### Prerequisites

| Tool | Required | Verify |
|------|----------|--------|
| Docker Desktop | Yes | `docker --version` |
| Docker Compose v2 | Yes | `docker compose version` |
| Java 17 | Local dev only | `java -version` |
| Python 3.11+ | Local dev only | `python --version` |
| k6 | Load testing only | `k6 version` |

> **Fully containerised**: `docker compose up --build` runs everything. Java/Python are only needed for running services outside Docker.

### Ports Used

| Port | Service |
|------|---------|
| 8080 | Payment Service |
| 8090 | API Gateway |
| 8000 | Fraud Service |
| 5433 | PostgreSQL |
| 29092 | Kafka (host access) |
| 6379 | Redis |
| 9090 | Prometheus |
| 3000 | Grafana |
| 3000+ | Frontend Dashboard (Vite dev server — auto-picks available port) |

---

## 5. Running the System

### Option A: Full Docker (Recommended)

```bash
# Clone the repository
cd payments-system

# Start all 8 services (first build takes ~2-4 min for Maven downloads)
docker compose up --build

# In a separate terminal, watch logs
docker compose logs -f payment-service fraud-service

# Verify all containers are healthy (~60-90s after build)
docker compose ps
```

Wait until `payments-payment-service` shows `healthy` status.

### Option B: Local Development (Hybrid)

```bash
# Start only infrastructure
docker compose up -d postgres kafka redis zookeeper fraud-service

# Run payment-service on host (uses localhost:29092 for Kafka)
cd payment-service
./mvnw spring-boot:run

# Run api-gateway on host (optional)
cd api-gateway
./mvnw spring-boot:run
```

### Health Checks

```bash
# Payment service
curl http://localhost:8080/actuator/health

# Fraud service
curl http://localhost:8000/health

# API Gateway
curl http://localhost:8090/actuator/health
```

### Shutdown

```bash
docker compose down          # Stop (data preserved in volumes)
docker compose down -v       # Stop + wipe all persistent data
```

---

## 6. Database Access

### Connection Details

| Parameter | Value |
|-----------|-------|
| Host | localhost |
| Port | 5433 |
| Database | payments |
| Username | user |
| Password | pass |

### Connect

```bash
docker exec -it payments-postgres psql -U user payments
```

### Useful SQL Queries

```sql
-- Recent payments
SELECT id, user_id, status, amount, currency, fraud_score, created_at
FROM payments ORDER BY created_at DESC LIMIT 20;

-- Count by status
SELECT status, COUNT(*) FROM payments GROUP BY status;

-- Outbox backlog (events not yet published)
SELECT id, event_type, status, retry_count, created_at
FROM outbox_events WHERE status != 'PUBLISHED' ORDER BY created_at;

-- Processed events (dedup table)
SELECT event_id, consumer_group, topic, processed_at
FROM processed_events ORDER BY processed_at DESC LIMIT 20;

-- Ledger entries for a payment
SELECT id, payment_id, entry_type, amount, currency, account_id, counter_account_id
FROM ledger_entries WHERE payment_id = '<payment-id>';

-- Verify ledger invariant (should always return 0)
SELECT payment_id,
       SUM(CASE WHEN entry_type = 'DEBIT' THEN amount ELSE 0 END) AS debits,
       SUM(CASE WHEN entry_type = 'CREDIT' THEN amount ELSE 0 END) AS credits,
       SUM(CASE WHEN entry_type = 'DEBIT' THEN amount ELSE 0 END) -
       SUM(CASE WHEN entry_type = 'CREDIT' THEN amount ELSE 0 END) AS imbalance
FROM ledger_entries GROUP BY payment_id HAVING
       SUM(CASE WHEN entry_type = 'DEBIT' THEN amount ELSE 0 END) !=
       SUM(CASE WHEN entry_type = 'CREDIT' THEN amount ELSE 0 END);

-- Webhook endpoints registered
SELECT id, merchant_id, url, events, active FROM webhook_endpoints;

-- Webhook delivery status
SELECT id, payment_id, event_type, status, attempt_count, last_error
FROM webhook_deliveries ORDER BY created_at DESC LIMIT 20;
```

---

## 7. Complete Testing Guide

### 7.1 — Create Payment (Normal Flow)

**Services Required:** Payment Service, PostgreSQL, Kafka, Redis, Fraud Service

**Steps:**

```bash
# 1. Create a payment
curl -s -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: test-normal-001" \
  -d '{
    "userId": "550e8400-e29b-41d4-a716-446655440000",
    "merchantId": "660e8400-e29b-41d4-a716-446655440000",
    "amount": 49.99,
    "currency": "USD",
    "description": "Normal test payment"
  }' | python -m json.tool
```

**Expected Response:**
```json
{
    "paymentId": "<uuid>",
    "userId": "550e8400-e29b-41d4-a716-446655440000",
    "merchantId": "660e8400-e29b-41d4-a716-446655440000",
    "amount": 49.99,
    "currency": "USD",
    "status": "PENDING",
    "idempotencyKey": "test-normal-001",
    "description": "Normal test payment",
    "createdAt": "<timestamp>"
}
```

**Validation:**

```bash
# 2. Wait 3-5 seconds for async fraud pipeline to complete, then poll
curl -s http://localhost:8080/payments/<paymentId> | python -m json.tool
# Expected: status should be "SUCCESS" (amount $49.99 is under fraud threshold)

# 3. Check DB
docker exec -it payments-postgres psql -U user payments \
  -c "SELECT id, status, fraud_score FROM payments WHERE idempotency_key = 'test-normal-001';"
# Expected: status = SUCCESS, fraud_score populated

# 4. Check ledger
docker exec -it payments-postgres psql -U user payments \
  -c "SELECT entry_type, amount, account_id FROM ledger_entries WHERE payment_id = '<paymentId>';"
# Expected: 1 DEBIT + 1 CREDIT row, both with amount 49.99

# 5. Check outbox was published
docker exec -it payments-postgres psql -U user payments \
  -c "SELECT event_type, status FROM outbox_events WHERE aggregate_id = '<paymentId>';"
# Expected: payment.created=PUBLISHED, payment.processed=PUBLISHED
```

---

### 7.2 — Idempotent Payment Replay

**Services Required:** Payment Service, PostgreSQL, Redis

**Steps:**

```bash
KEY="idem-replay-$(date +%s)"

# 1. First request — creates the payment
curl -s -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $KEY" \
  -d '{"userId":"550e8400-e29b-41d4-a716-446655440000","merchantId":"660e8400-e29b-41d4-a716-446655440000","amount":100,"currency":"USD","description":"Idem test"}' \
  | python -m json.tool

# Save the paymentId from the response
# PAYMENT_ID=<value from response>

# 2. Replay with the SAME idempotency key
curl -s -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $KEY" \
  -d '{"userId":"550e8400-e29b-41d4-a716-446655440000","merchantId":"660e8400-e29b-41d4-a716-446655440000","amount":100,"currency":"USD","description":"Idem test"}' \
  | python -m json.tool
```

**Expected:**
- Both responses return the **same paymentId**
- HTTP status: 201 for both (controller always returns 201)

**Validation:**

```sql
-- Only ONE row should exist in the database
SELECT COUNT(*) FROM payments WHERE idempotency_key = '<KEY>';
-- Expected: 1

-- Only ONE outbox event for payment.created
SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = '<paymentId>' AND event_type = 'payment.created';
-- Expected: 1
```

---

### 7.3 — Fraud Rejection Flow

**Services Required:** Payment Service, PostgreSQL, Kafka, Fraud Service

**Steps:**

```bash
# Create a high-value payment (>$10,000 triggers rule-based fraud rejection)
curl -s -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: fraud-test-001" \
  -d '{
    "userId": "550e8400-e29b-41d4-a716-446655440000",
    "merchantId": "660e8400-e29b-41d4-a716-446655440000",
    "amount": 15000.00,
    "currency": "USD",
    "description": "High-value fraud test"
  }' | python -m json.tool
```

**Expected:** status=PENDING initially

```bash
# Wait 3-5 seconds, then poll
curl -s http://localhost:8080/payments/<paymentId> | python -m json.tool
```

**Expected:** status=FRAUD_REJECTED, fraudScore=0.95

**Validation:**

```sql
-- Payment should be FRAUD_REJECTED
SELECT status, fraud_score, failure_reason FROM payments WHERE idempotency_key = 'fraud-test-001';
-- Expected: FRAUD_REJECTED, 0.95, "FRAUD: amount_exceeds_threshold"

-- NO ledger entries should exist (fraud rejected before ledger write)
SELECT COUNT(*) FROM ledger_entries WHERE payment_id = '<paymentId>';
-- Expected: 0

-- Outbox should have payment.failed event
SELECT event_type, status FROM outbox_events WHERE aggregate_id = '<paymentId>';
-- Expected: payment.created=PUBLISHED, payment.failed=PUBLISHED, fraud.alerts=PUBLISHED
```

---

### 7.4 — Unsupported Currency Rejection

**Services Required:** Payment Service, PostgreSQL, Kafka, Fraud Service

**Steps:**

```bash
curl -s -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: currency-test-001" \
  -d '{
    "userId": "550e8400-e29b-41d4-a716-446655440000",
    "merchantId": "660e8400-e29b-41d4-a716-446655440000",
    "amount": 500.00,
    "currency": "XYZ",
    "description": "Unsupported currency test"
  }' | python -m json.tool
```

**Expected:** After 3-5 seconds, status=FRAUD_REJECTED, reason contains "unsupported_currency"

---

### 7.5 — Ledger Consistency Validation

**Services Required:** Payment Service, PostgreSQL

**Steps:**

```bash
# 1. Create several successful payments
for i in $(seq 1 5); do
  curl -s -X POST http://localhost:8080/payments \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: ledger-test-$i" \
    -d "{\"userId\":\"550e8400-e29b-41d4-a716-446655440000\",\"merchantId\":\"660e8400-e29b-41d4-a716-446655440000\",\"amount\":$((i * 100)),\"currency\":\"USD\",\"description\":\"Ledger test $i\"}"
  sleep 1
done

# 2. Wait 10 seconds for all fraud checks to complete
sleep 10
```

**Validation:**

```sql
-- Every SUCCESS payment must have exactly 2 ledger entries
SELECT p.id, p.status, COUNT(l.id) AS ledger_count
FROM payments p
LEFT JOIN ledger_entries l ON l.payment_id = p.id
WHERE p.status = 'SUCCESS'
GROUP BY p.id, p.status
HAVING COUNT(l.id) != 2;
-- Expected: 0 rows (no inconsistencies)

-- Verify debits == credits globally
SELECT payment_id,
       SUM(CASE WHEN entry_type = 'DEBIT' THEN amount ELSE 0 END) AS debits,
       SUM(CASE WHEN entry_type = 'CREDIT' THEN amount ELSE 0 END) AS credits
FROM ledger_entries GROUP BY payment_id
HAVING SUM(CASE WHEN entry_type = 'DEBIT' THEN amount ELSE 0 END) !=
       SUM(CASE WHEN entry_type = 'CREDIT' THEN amount ELSE 0 END);
-- Expected: 0 rows (perfect balance)
```

---

### 7.6 — Kafka Duplicate Event Handling

**Services Required:** Payment Service, PostgreSQL, Kafka

**Scenario:** The same fraud.result event is delivered twice (simulating Kafka redelivery after consumer restart).

**Steps:**

```bash
# 1. Create a payment and wait for it to complete
curl -s -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: dedup-test-001" \
  -d '{"userId":"550e8400-e29b-41d4-a716-446655440000","merchantId":"660e8400-e29b-41d4-a716-446655440000","amount":200,"currency":"USD","description":"Dedup test"}'

sleep 10

# 2. Check the processed_events table
docker exec -it payments-postgres psql -U user payments \
  -c "SELECT event_id, consumer_group, topic FROM processed_events WHERE topic = 'fraud.result' ORDER BY processed_at DESC LIMIT 5;"
```

**Expected:** Each eventId appears exactly ONCE in processed_events. If Kafka redelivers, the `IdempotentConsumerService` blocks the duplicate via `DataIntegrityViolationException` on the PK constraint.

**Validation:**

```sql
-- No duplicate processed events
SELECT event_id, COUNT(*) FROM processed_events GROUP BY event_id HAVING COUNT(*) > 1;
-- Expected: 0 rows

-- Each SUCCESS payment has exactly 2 ledger entries (no double-write)
SELECT payment_id, COUNT(*) FROM ledger_entries GROUP BY payment_id HAVING COUNT(*) > 2;
-- Expected: 0 rows
```

---

### 7.7 — Outbox Replay Scenario

**Scenario:** If Kafka was temporarily unavailable when a payment was created, the outbox poller retries on recovery.

**Steps:**

```bash
# 1. Stop Kafka
docker compose stop kafka

# 2. Create a payment (DB write succeeds, Kafka publish will fail)
curl -s -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: outbox-test-001" \
  -d '{"userId":"550e8400-e29b-41d4-a716-446655440000","merchantId":"660e8400-e29b-41d4-a716-446655440000","amount":300,"currency":"USD","description":"Outbox test"}'

# 3. Check outbox — event should be NEW (not yet published)
docker exec -it payments-postgres psql -U user payments \
  -c "SELECT event_type, status, retry_count FROM outbox_events WHERE aggregate_id IN (SELECT id::text FROM payments WHERE idempotency_key = 'outbox-test-001');"
# Expected: status=NEW, retry_count may increment

# 4. Restart Kafka
docker compose start kafka
sleep 15

# 5. Check outbox again — should now be PUBLISHED
docker exec -it payments-postgres psql -U user payments \
  -c "SELECT event_type, status FROM outbox_events WHERE aggregate_id IN (SELECT id::text FROM payments WHERE idempotency_key = 'outbox-test-001');"
# Expected: status=PUBLISHED

# 6. Payment should eventually complete
curl -s http://localhost:8080/payments/<paymentId> | python -m json.tool
# Expected: status=SUCCESS after fraud pipeline processes
```

---

### 7.8 — Rate Limiting

**Services Required:** Payment Service, Redis

**Steps:**

```bash
# Send > 100 requests in under 60 seconds
for i in $(seq 1 110); do
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST http://localhost:8080/payments \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: rate-test-$i" \
    -d '{"userId":"550e8400-e29b-41d4-a716-446655440000","merchantId":"660e8400-e29b-41d4-a716-446655440000","amount":10,"currency":"USD","description":"Rate test"}')
  echo "Request $i: HTTP $STATUS"
done
```

**Expected:** First ~100 requests return 201. After limit is hit, requests return `429 Too Many Requests`.

**Validation:**

```bash
# Response body for 429:
# {"type":"RATE_LIMIT_EXCEEDED","message":"Rate limit exceeded. Max 100 requests per minute.","status":429,...}
```

---

### 7.9 — Invalid Input Handling

**Services Required:** Payment Service

**Steps:**

```bash
# Missing required field
curl -s -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: invalid-001" \
  -d '{"merchantId":"660e8400-e29b-41d4-a716-446655440000","amount":10,"currency":"USD"}' \
  | python -m json.tool
# Expected: 400 — "userId: userId is required"

# Amount = 0
curl -s -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: invalid-002" \
  -d '{"userId":"550e8400-e29b-41d4-a716-446655440000","merchantId":"660e8400-e29b-41d4-a716-446655440000","amount":0,"currency":"USD"}' \
  | python -m json.tool
# Expected: 400 — "amount: Amount must be greater than zero"

# Missing Idempotency-Key header
curl -s -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -d '{"userId":"550e8400-e29b-41d4-a716-446655440000","merchantId":"660e8400-e29b-41d4-a716-446655440000","amount":10,"currency":"USD"}' \
  | python -m json.tool
# Expected: 400 — "Required header missing: Idempotency-Key"

# Currency code too long
curl -s -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: invalid-003" \
  -d '{"userId":"550e8400-e29b-41d4-a716-446655440000","merchantId":"660e8400-e29b-41d4-a716-446655440000","amount":10,"currency":"USDD"}' \
  | python -m json.tool
# Expected: 400 — "currency: Currency must be a 3-letter ISO code"

# Non-existent payment ID
curl -s http://localhost:8080/payments/00000000-0000-0000-0000-000000000000 | python -m json.tool
# Expected: 404 — "PAYMENT_NOT_FOUND"
```

---

### 7.10 — Trace ID / Correlation ID Propagation

**Services Required:** Payment Service, Kafka, Fraud Service

**Steps:**

```bash
# Send request with explicit trace/correlation IDs
curl -s -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: trace-test-001" \
  -H "X-Trace-ID: my-trace-id-12345" \
  -H "X-Correlation-ID: my-correlation-id-67890" \
  -d '{"userId":"550e8400-e29b-41d4-a716-446655440000","merchantId":"660e8400-e29b-41d4-a716-446655440000","amount":50,"currency":"USD","description":"Trace test"}' \
  -v 2>&1 | grep -i "x-trace\|x-correlation"
```

**Expected Response Headers:**
```
X-Trace-ID: my-trace-id-12345
X-Correlation-ID: my-correlation-id-67890
```

**Validation:**

```bash
# Check payment-service logs for the trace IDs
docker compose logs payment-service | grep "my-trace-id-12345"
# Expected: traceId=my-trace-id-12345 appears in log entries

# Check fraud-service logs
docker compose logs fraud-service | grep "my-trace-id-12345" || echo "trace_id forwarded via Kafka headers"
```

```sql
-- Verify stored in DB
SELECT trace_id, correlation_id FROM payments WHERE idempotency_key = 'trace-test-001';
-- Expected: trace_id = my-trace-id-12345, correlation_id = my-correlation-id-67890

-- Verify in outbox
SELECT trace_id, correlation_id FROM outbox_events
WHERE aggregate_id IN (SELECT id::text FROM payments WHERE idempotency_key = 'trace-test-001');
-- Expected: same values propagated
```

---

### 7.11 — Fraud Service HTTP Endpoint (Direct)

**Services Required:** Fraud Service

**Steps:**

```bash
# Clean transaction
curl -s -X POST http://localhost:8000/score \
  -H "Content-Type: application/json" \
  -d '{"payment_id":"p1","user_id":"u1","amount":100,"currency":"USD"}' \
  | python -m json.tool
# Expected: fraud=false, score < 0.75, stage="ml"

# High-value fraud
curl -s -X POST http://localhost:8000/score \
  -H "Content-Type: application/json" \
  -d '{"payment_id":"p2","user_id":"u2","amount":15000,"currency":"USD"}' \
  | python -m json.tool
# Expected: fraud=true, score=0.95, reason="amount_exceeds_threshold", stage="rule"

# Unsupported currency
curl -s -X POST http://localhost:8000/score \
  -H "Content-Type: application/json" \
  -d '{"payment_id":"p3","user_id":"u3","amount":500,"currency":"XYZ"}' \
  | python -m json.tool
# Expected: fraud=true, score=0.95, reason="unsupported_currency", stage="rule"
```

---

### 7.12 — Webhook Registration and Delivery

**Services Required:** Payment Service, PostgreSQL

**Steps:**

```bash
# 1. Register a webhook endpoint
curl -s -X POST http://localhost:8080/webhooks \
  -H "Content-Type: application/json" \
  -d '{
    "merchantId": "660e8400-e29b-41d4-a716-446655440000",
    "url": "https://httpbin.org/post",
    "events": "payment.processed,payment.failed"
  }' | python -m json.tool
# Note the returned "secret" — used for HMAC verification

# 2. Create a payment for that merchant
curl -s -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: webhook-test-001" \
  -d '{"userId":"550e8400-e29b-41d4-a716-446655440000","merchantId":"660e8400-e29b-41d4-a716-446655440000","amount":75,"currency":"USD","description":"Webhook test"}'

# 3. Wait for processing, then check webhook deliveries
sleep 10
docker exec -it payments-postgres psql -U user payments \
  -c "SELECT event_type, status, attempt_count, last_response_code FROM webhook_deliveries ORDER BY created_at DESC LIMIT 5;"
```

**Expected:** One webhook_delivery row with status=SUCCESS (if httpbin is reachable) or FAILED/PENDING (if not).

---

### 7.13 — DLQ Routing

**Scenario:** An unparseable message on a topic routes to the DLQ.

**Steps:**

```bash
# Produce a malformed message to fraud.result
docker exec payments-kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic fraud.result <<< '{"invalid": "json without required fields"}'

sleep 5

# Check that it was routed to the DLQ
docker exec payments-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic fraud.result.DLQ \
  --from-beginning --max-messages 1 --timeout-ms 5000
# Expected: The malformed message appears in fraud.result.DLQ
```

---

### 7.14 — Kafka Topic Inspection

**Steps:**

```bash
# List all topics
docker exec payments-kafka kafka-topics --bootstrap-server localhost:9092 --list

# Consumer group status
docker exec payments-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 --describe --group payment-processor-group

# Tail payment.created events
docker exec payments-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic payment.created --from-beginning --max-messages 5
```

---

### 7.15 — WebSocket Real-Time Updates

**Scenario:** Connect to WebSocket, create a payment, observe live status update.

**Steps:**

```bash
# 1. Connect to WebSocket (using websocat or wscat)
# Install: npm install -g wscat
wscat -c ws://localhost:8080/ws/payments

# 2. In another terminal, create a payment
curl -s -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: ws-test-001" \
  -d '{"userId":"550e8400-e29b-41d4-a716-446655440000","merchantId":"660e8400-e29b-41d4-a716-446655440000","amount":50,"currency":"USD","description":"WebSocket test"}'

# 3. Observe: the WebSocket should receive a JSON message with the
#    payment.processed event within ~3-5 seconds
```

---

### 7.16 — Reconciliation Service

**Scenario:** The reconciliation job detects stale PENDING payments.

**Steps:**

```bash
# 1. Stop fraud-service so payments stay PENDING
docker compose stop fraud-service

# 2. Create a payment
curl -s -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: recon-test-001" \
  -d '{"userId":"550e8400-e29b-41d4-a716-446655440000","merchantId":"660e8400-e29b-41d4-a716-446655440000","amount":50,"currency":"USD","description":"Reconciliation test"}'

# 3. Wait for reconciliation to fire (runs every 30 min by default)
#    Or check the logs for when it last ran:
docker compose logs payment-service | grep "RECONCILIATION"
# Expected: STALE_PAYMENT alert for this payment after stale-payment-threshold-minutes (10 min)

# 4. Restart fraud-service
docker compose start fraud-service
```

---

## 8. Failure Testing Guide

### 8.1 — Kafka Down

```bash
# Stop Kafka
docker compose stop kafka

# Create a payment — succeeds (writes to DB + outbox)
curl -s -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: kafka-down-001" \
  -d '{"userId":"550e8400-e29b-41d4-a716-446655440000","merchantId":"660e8400-e29b-41d4-a716-446655440000","amount":100,"currency":"USD","description":"Kafka down test"}'
# Expected: 201 Created (DB write succeeds)

# Check outbox — event stays NEW
docker exec -it payments-postgres psql -U user payments \
  -c "SELECT event_type, status, retry_count FROM outbox_events ORDER BY created_at DESC LIMIT 3;"

# Restart Kafka
docker compose start kafka
sleep 15

# Outbox poller re-publishes within 1 second of Kafka being reachable
docker exec -it payments-postgres psql -U user payments \
  -c "SELECT event_type, status FROM outbox_events ORDER BY created_at DESC LIMIT 3;"
# Expected: status=PUBLISHED

# Payment processes normally
curl -s http://localhost:8080/payments/<paymentId> | python -m json.tool
# Expected: status=SUCCESS (eventually)
```

**System Behavior:**
- POST /payments still works (writes to DB)
- Outbox events accumulate as NEW (retry_count increments)
- After Kafka recovers, poller publishes backlog within seconds
- No data loss, no duplicates

---

### 8.2 — Redis Down

```bash
# Stop Redis
docker compose stop redis

# Create a payment — rate limiter falls back to Caffeine in-memory
curl -s -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: redis-down-001" \
  -d '{"userId":"550e8400-e29b-41d4-a716-446655440000","merchantId":"660e8400-e29b-41d4-a716-446655440000","amount":100,"currency":"USD","description":"Redis down test"}'
# Expected: 201 Created — rate limiting uses in-memory fallback

# Check logs for fallback activation
docker compose logs payment-service | grep "Redis unavailable"
# Expected: "Redis unavailable for rate limiting... using in-memory fallback"

# Restart Redis
docker compose start redis
# RateLimiterService automatically retries Redis on next request
```

**System Behavior:**
- Rate limiting degrades to per-JVM Caffeine cache (50K entries, 2-min TTL)
- Idempotency cache misses → falls back to DB pessimistic lock
- Payment cache misses → falls back to DB read
- WebSocket pub/sub paused until Redis recovers

---

### 8.3 — Fraud Service Down

```bash
# Stop fraud-service
docker compose stop fraud-service

# Create a payment
curl -s -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: fraud-down-001" \
  -d '{"userId":"550e8400-e29b-41d4-a716-446655440000","merchantId":"660e8400-e29b-41d4-a716-446655440000","amount":100,"currency":"USD","description":"Fraud down test"}'
# Expected: 201 Created — payment saved as PENDING

# Payment stays PENDING (fraud.request events accumulate in Kafka)
curl -s http://localhost:8080/payments/<paymentId> | python -m json.tool
# Expected: status=PENDING (no fraud result yet)

# Restart fraud-service
docker compose start fraud-service
sleep 15

# Fraud service consumes the backlog from Kafka
curl -s http://localhost:8080/payments/<paymentId> | python -m json.tool
# Expected: status=SUCCESS (fraud scored after recovery)
```

**System Behavior:**
- Payments are accepted and saved (PENDING)
- `fraud.request` events queue in Kafka (retained 168 hours)
- On fraud-service recovery, backlog is consumed in parallel (3 workers)
- No data loss, no double-processing

---

### 8.4 — PostgreSQL Down

```bash
# Stop PostgreSQL
docker compose stop postgres

# Attempt to create a payment
curl -s -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: db-down-001" \
  -d '{"userId":"550e8400-e29b-41d4-a716-446655440000","merchantId":"660e8400-e29b-41d4-a716-446655440000","amount":100,"currency":"USD","description":"DB down test"}'
# Expected: 500 Internal Server Error (cannot persist payment)

# Restart PostgreSQL
docker compose start postgres
sleep 10

# HikariCP reconnects automatically within 30 seconds
curl -s -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: db-recovery-001" \
  -d '{"userId":"550e8400-e29b-41d4-a716-446655440000","merchantId":"660e8400-e29b-41d4-a716-446655440000","amount":100,"currency":"USD","description":"DB recovery test"}'
# Expected: 201 Created (DB recovered)
```

---

### 8.5 — Consumer Restart (Idempotency Test)

```bash
# 1. Create multiple payments
for i in $(seq 1 5); do
  curl -s -X POST http://localhost:8080/payments \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: restart-test-$i" \
    -d "{\"userId\":\"550e8400-e29b-41d4-a716-446655440000\",\"merchantId\":\"660e8400-e29b-41d4-a716-446655440000\",\"amount\":$((i * 50)),\"currency\":\"USD\",\"description\":\"Restart test $i\"}"
done

# 2. Immediately restart the payment-service container
docker compose restart payment-service
sleep 30

# 3. Check that all payments reach terminal state (no stuck PENDING)
docker exec -it payments-postgres psql -U user payments \
  -c "SELECT status, COUNT(*) FROM payments WHERE idempotency_key LIKE 'restart-test-%' GROUP BY status;"
# Expected: All either SUCCESS or FRAUD_REJECTED (no PENDING left)

# 4. No duplicate ledger entries
docker exec -it payments-postgres psql -U user payments \
  -c "SELECT payment_id, COUNT(*) FROM ledger_entries GROUP BY payment_id HAVING COUNT(*) > 2;"
# Expected: 0 rows
```

---

## 9. Load Testing Guide

### Install k6

```bash
# macOS
brew install k6

# Windows
choco install k6

# Linux
sudo gpg -k
sudo gpg --no-default-keyring --keyring /usr/share/keyrings/k6-archive-keyring.gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys C5AD17C747E3415A3642D57D77C6C491D6AC1D68
echo "deb [signed-by=/usr/share/keyrings/k6-archive-keyring.gpg] https://dl.k6.io/deb stable main" | sudo tee /etc/apt/sources.list.d/k6.list
sudo apt-get update && sudo apt-get install k6
```

### Run Load Test

```bash
# Default profile (direct to payment-service — no auth required)
k6 run load-tests/payment-load-test.js

# Custom load
k6 run --vus 50 --duration 60s load-tests/payment-load-test.js

# Via API Gateway (JWT required)
k6 run --env BASE_URL=http://localhost:8090/api --env JWT_TOKEN=$JWT_TOKEN \
  load-tests/payment-load-test.js
```

### Default Load Profile

| Phase | Duration | Target VUs | Purpose |
|-------|----------|-----------|---------|
| Ramp-up | 0-30s | 0 → 10 | Warm caches, JIT |
| Sustained | 30-90s | 10 → 50 | Steady-state performance |
| Spike | 90-120s | 50 → 100 | Stress test |
| Scale down | 120-150s | 100 → 10 | Recovery validation |
| Cool down | 150-160s | 10 → 0 | Clean shutdown |

### Thresholds (test fails if violated)

| Metric | Threshold | Description |
|--------|-----------|-------------|
| `payment_create_duration` | p(95) < 500ms | POST /payments latency |
| `payment_get_duration` | p(95) < 200ms | GET /payments/:id (cache hit) |
| `http_req_failed` | rate < 0.01 | Error rate (429s excluded) |
| `idempotency_check_pass` | rate > 0.99 | Idempotency replay success |

### What to Monitor During Load

- **Prometheus** (http://localhost:9090): `payments_created_total`, `fraud_detected_total`
- **Grafana** (http://localhost:3000): JVM heap, Kafka consumer lag, p95 latencies
- **Logs**: `docker compose logs -f payment-service` — look for rate limit warnings, Kafka errors
- **DB pool**: `SELECT count(*) FROM pg_stat_activity WHERE datname = 'payments';`

---

## 10. Observability

### Logs

All logs include `traceId` and `correlationId` via SLF4J MDC:

```json
{"@timestamp":"...","level":"INFO","traceId":"abc123","correlationId":"def456",
 "message":"Payment created: paymentId=xyz789"}
```

```bash
docker compose logs -f payment-service
docker compose logs -f fraud-service
```

### Prometheus Metrics

Access: http://localhost:9090

| Metric | Type | Service |
|--------|------|---------|
| `payments_created_total` | Counter | Payment Service |
| `payments_processed_total` | Counter | Payment Service |
| `fraud_detected_total` | Counter | Payment Service |
| `payment_processing_seconds` | Timer | Payment Service |
| `fraud_requests_total` | Counter | Fraud Service |
| `fraud_request_duration_seconds` | Histogram | Fraud Service |
| `fraud_score_distribution` | Histogram | Fraud Service |
| `http_server_requests_seconds` | Histogram | Spring Boot |

### Grafana Dashboards

1. Open http://localhost:3000 (admin / admin)
2. Add data source → Prometheus → URL: `http://prometheus:9090`
3. Import dashboards:
   - JVM (Micrometer): ID `4701`
   - Spring Boot: ID `12900`
   - Kafka: ID `7589`

### Prometheus Configuration

Prometheus scrapes:
- `payment-service:8080/actuator/prometheus` (every 15s)
- `fraud-service:8000/metrics` (every 15s)

---

## 11. Troubleshooting

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| `payment-service` container never becomes healthy | Maven downloading deps on first Docker build (~2-4 min) | Wait; check `docker compose logs payment-service` |
| `Connection refused` to Kafka from host | Kafka external listener is on port 29092, not 9092 | Use `localhost:29092` from host machine |
| Payments stuck in `PENDING` forever | Fraud service not healthy or Kafka backlog | `docker compose ps fraud-service`; wait or restart |
| `fraud.request` events not consumed | Fraud service still starting | Check logs: `docker compose logs fraud-service` — wait for "Application startup complete" |
| `401 Unauthorized` from API Gateway | Missing or expired JWT | Generate new HS256 JWT with `GATEWAY_JWT_SECRET` |
| `429 Too Many Requests` | Rate limit hit (100 req/min per user) | Wait 60s or increase `app.rate-limiting.requests-per-minute` |
| OutboxEvent status stuck at `FAILED` | Kafka unreachable for > max retries (3) | Check Kafka; failed events require manual re-queue or re-create |
| `spring.jpa.open-in-view` warning in logs | Spring default (harmless) | Already set to `false` in application.yml |
| Webhook delivery stuck in `FAILED` | Destination URL unreachable | Check `webhook_deliveries` table; retry scheduler runs every 30s |
| `DataIntegrityViolationException` on payment create | Race condition on idempotency key | Expected behavior — `GlobalExceptionHandler` returns 409 Conflict |
| Redis `Connection refused` in logs | Redis container not started | `docker compose up -d redis` |
| High Kafka consumer lag | Slow processing or too few consumer threads | Check `kafka-consumer-groups --describe`; increase `concurrency` in KafkaConfig |
| `CRITICAL: Failed to send to DLQ` in logs | Kafka completely unavailable | Manual intervention required — check Kafka cluster health |

### Useful Debugging Commands

```bash
# Container status
docker compose ps

# Kafka consumer group lag
docker exec payments-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group payment-processor-group

# PostgreSQL connections
docker exec -it payments-postgres psql -U user payments \
  -c "SELECT count(*) FROM pg_stat_activity WHERE datname = 'payments';"

# Redis keys
docker exec payments-redis redis-cli KEYS "rate_limit:*"
docker exec payments-redis redis-cli KEYS "idem:*"
docker exec payments-redis redis-cli KEYS "payment:*"

# Tail all DLQ topics
docker exec payments-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --whitelist ".*\.DLQ" --from-beginning --max-messages 10

# Force reconciliation (trigger via log check)
docker compose logs payment-service | grep "Reconciliation"
```

---

## Configuration Reference

| Variable | Local Default | Docker Value | Purpose |
|----------|--------------|-------------|---------|
| `SPRING_DATASOURCE_URL` | `jdbc:postgresql://localhost:5433/payments` | `jdbc:postgresql://postgres:5432/payments` | PostgreSQL URL |
| `SPRING_DATASOURCE_USERNAME` | `user` | `user` | DB username |
| `SPRING_DATASOURCE_PASSWORD` | `pass` | `pass` | DB password |
| `SPRING_KAFKA_BOOTSTRAP_SERVERS` | `localhost:29092` | `kafka:9092` | Kafka bootstrap |
| `SPRING_DATA_REDIS_HOST` | `localhost` | `redis` | Redis host |
| `SPRING_DATA_REDIS_PORT` | `6379` | `6379` | Redis port |
| `FRAUD_SERVICE_URL` | `http://localhost:8000` | `http://fraud-service:8000` | Fraud service URL |
| `GATEWAY_JWT_SECRET` | `changeme-replace-in-production-min-32-chars` | *(set via env)* | JWT signing secret |

### Kafka Listener Architecture

| Listener | Host Access | Container Access |
|----------|------------|-----------------|
| INTERNAL | N/A | `kafka:9092` |
| EXTERNAL | `localhost:29092` | N/A |

---

## 12. Frontend Dashboard

A real-time system visualization dashboard built with React, TypeScript, Tailwind CSS, and native WebSocket.

### Quick Start

```bash
cd frontend
npm install
npm run dev
```

The Vite dev server starts on an available port (default 3000, auto-increments if Grafana is running). API calls proxy to the gateway at `localhost:8090`.

### What It Shows

| Panel | Description |
|---|---|
| **Metrics Bar** | Total, Success, Pending, Failed, Fraud counts + success rate — derived from stored events, always accurate |
| **Live Payment Stream** | Real-time table of every event via WebSocket (paymentId, userId, amount, status, event type, timestamp) |
| **System Status** | Green/red indicators for API Gateway health and WebSocket connectivity, last event timestamp |
| **Create Payment** | Form to submit payments (amount, currency, idempotency key) with instant response display |
| **Event Stream** | Kafka-style timeline: `payment.created` → `fraud.result` → `payment.processed` |
| **Alerts & Fraud** | Fraud rejections and failed payment alerts |

### Simulation Controls

- **↻ Duplicate Request** — Resends with the same idempotency key to demonstrate exactly-once semantics
- **⚡ Burst (5 rapid)** — Fires 5 concurrent payments to test parallel processing and rate limiting
- **XYZ currency** — Select from the currency dropdown to trigger fraud detection (unsupported currency rule)
- **Amount > $10,000** — Triggers the fraud amount-threshold rule

### Data Persistence

Events and alerts are stored in `sessionStorage`. Refreshing the page restores all data. Metrics are computed from stored events (not incremental counters), so they are always accurate after refresh.

### Architecture

```
Browser ──► Vite Dev Server (:3000)
             ├─ /api/* ──proxy──► API Gateway (:8090) ──► Payment Service (:8080)
             └─ ws://localhost:8080/ws/payments (direct WebSocket)
```

### Tech Stack

- React 19 + TypeScript (Vite 6)
- Tailwind CSS 4
- Axios (HTTP client with JWT auth)
- Native WebSocket (auto-reconnect, ping/pong keepalive)
- No heavy state libraries — `useState` + `useMemo` + `sessionStorage`

For full usage instructions, see [`frontend/README.md`](frontend/README.md).

---

## Security Notes

- Override `GATEWAY_JWT_SECRET` and database credentials before any non-local deployment
- All Docker containers run as **non-root users** (Java services)
- CORS and TLS are not configured — add a reverse proxy (nginx/Caddy) for production
- The API Gateway strips the `Authorization` header before forwarding to downstream services
- Webhook payloads should be HMAC-signed in production (signing infrastructure scaffolded in `WebhookEndpoint.secret`)
