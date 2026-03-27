# Architecture

## System Overview

This system implements a complete payment processing pipeline using event-driven microservices:

- **Payment Service** (Java 17 / Spring Boot 3.4.4) — accepts payments, enforces idempotency, writes to PostgreSQL, publishes events via the outbox pattern
- **Fraud Service** (Python 3.11 / FastAPI) — scores transactions using a rule engine + IsolationForest ML model, communicates via Kafka
- **API Gateway** (Spring Cloud Gateway) — JWT authentication, Redis-backed rate limiting, circuit breakers
- **Kafka** — event streaming backbone (`payment.created → fraud.request → fraud.result → payment.processed`)
- **PostgreSQL** — durable storage for payments, ledger entries, outbox events, processed events, webhooks
- **Redis** — caching (idempotency keys, payment responses), rate limiting, WebSocket pub/sub

## Design Patterns

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

## Architecture Diagram

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

## Service Components

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
| Kafka Consumers | `PaymentEventConsumer`, `FraudResultConsumer`, `PaymentProcessedConsumer` | Event-driven processing pipeline |
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
| Kafka consumer loop | Async `fraud.request` → `fraud.result` pipeline |
| Rule engine | Velocity check, amount threshold, currency allowlist |
| ML model | IsolationForest trained on synthetic data at startup |
| `/health` | Docker healthcheck endpoint |
| `/metrics` | Prometheus metrics (`fraud_requests_total`, etc.) |

### API Gateway (Spring Cloud Gateway)

| Component | Responsibility |
|-----------|---------------|
| JWT validation | HS256 symmetric key (`GATEWAY_JWT_SECRET`) |
| Rate limiting | Redis-backed token bucket (100 rps, burst 200) |
| Request enrichment | Injects `X-Correlation-ID`, `X-User-Id` headers |
| Circuit breaker | Opens on downstream failures, fallback endpoint |
| Retry filter | 3 retries on 502/503/504 for GET requests |

### Frontend Dashboard (React / TypeScript / Tailwind CSS)

| Panel | Description |
|---|---|
| Metrics Bar | Total, Success, Pending, Failed, Fraud counts + success rate |
| Live Payment Stream | Real-time table of events via WebSocket |
| System Status | Health indicators for API Gateway and WebSocket connectivity |
| Create Payment | Form to submit payments with instant response display |
| Event Stream | Kafka-style timeline: `payment.created` → `fraud.result` → `payment.processed` |
| Alerts & Fraud | Fraud rejections and failed payment alerts |
