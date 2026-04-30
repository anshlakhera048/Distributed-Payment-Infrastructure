# Distributed Payments System

A production-grade distributed payments platform demonstrating event-driven microservices, the Transactional Outbox Pattern, real-time ML fraud detection, and exactly-once processing semantics — all orchestrated via Docker Compose.

## Project Overview

This system processes financial payments through a multi-stage pipeline: API ingestion → fraud detection → ledger settlement. It solves the core challenge of building a **reliable, consistent, and scalable payment processor** that guarantees no duplicate charges, no lost transactions, and real-time fraud prevention — even under infrastructure failures.

The architecture prioritizes correctness (exactly-once semantics, double-entry ledger) while maintaining high throughput (1000+ TPS) through asynchronous event-driven processing, adaptive backpressure, and horizontal scaling via Kafka partitioning.

## Tech Stack

### Backend
| Component | Technology | Purpose |
|-----------|-----------|---------|
| Payment Service | Java 17, Spring Boot 3.4.4, Maven | Core payment processing, ledger, outbox |
| API Gateway | Java 17, Spring Cloud Gateway | JWT auth, rate limiting, routing |
| Fraud Service | Python 3.11, FastAPI | ML-based fraud detection pipeline |

### Infrastructure
| Component | Technology | Purpose |
|-----------|-----------|---------|
| Database | PostgreSQL 15 | Payments, ledger, outbox events |
| Message Broker | Apache Kafka (Confluent 7.5) | Event streaming, async fraud pipeline |
| Cache | Redis 7 | Rate limiting, idempotency cache, WebSocket pub/sub |
| Monitoring | Prometheus + Grafana | Metrics collection and dashboards |

### Frontend
| Component | Technology | Purpose |
|-----------|-----------|---------|
| Dashboard | React 19, TypeScript, Vite, Tailwind CSS | Real-time payment monitoring |

### Key Libraries & Patterns
- **Resilience4j** — Circuit breaker, rate limiter
- **Spring Retry** — Transient failure recovery
- **Micrometer** — Prometheus metrics export
- **scikit-learn** — IsolationForest fraud scoring model
- **confluent-kafka** — Python Kafka consumer/producer
- **k6** — Load testing

## Architecture Overview

```
┌─────────────┐     ┌──────────────────┐     ┌──────────────────────────────────────┐
│  Frontend   │────▶│   API Gateway    │────▶│         Payment Service              │
│  (React)    │     │ JWT + Rate Limit │     │                                      │
└─────────────┘     └──────────────────┘     │  ┌─────────┐  ┌────────┐  ┌───────┐ │
                                             │  │ Outbox  │  │ Ledger │  │ Cache │ │
                                             │  └────┬────┘  └────────┘  └───────┘ │
                                             └───────┼─────────────────────────────-┘
                                                     │
                              ┌───────────────────────┼────────────────────────┐
                              │          Kafka        │                        │
                              │   payment.created ────┘                        │
                              │   fraud.request  ──▶  Fraud Service (Python)   │
                              │   fraud.result   ◀──  ML + Rules               │
                              └────────────────────────────────────────────────┘
                                                     │
                              ┌───────────────────────┼────────────────────────┐
                              │       PostgreSQL      │       Redis             │
                              │  payments, ledger,    │  rate limits, cache,    │
                              │  outbox_events,       │  WebSocket pub/sub      │
                              │  processed_events     │                         │
                              └───────────────────────┴────────────────────────┘
```

**Flow:**
1. Client sends payment request → API Gateway validates JWT + enforces rate limit
2. Payment Service creates payment (PENDING), writes outbox event in same DB transaction
3. Outbox Publisher polls and publishes `payment.created` to Kafka
4. Payment Event Consumer reads event, sends `fraud.request` to Kafka
5. Fraud Service (Python) scores transaction (rules + ML), publishes `fraud.result`
6. Fraud Result Consumer finalizes payment: SUCCESS (with ledger entry) or FRAUD_REJECTED
7. WebSocket broadcasts real-time status updates to frontend dashboard

## Setup Instructions

### Prerequisites

- **Docker & Docker Compose** (v2.x) — required for all deployment modes
- **Java 17+** — only for local dev mode
- **Python 3.11+** — only for local dev mode
- **Node.js 18+** — only for frontend dev
- **Maven 3.9+** — only for local dev mode

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SPRING_DATASOURCE_URL` | `jdbc:postgresql://localhost:5433/payments` | PostgreSQL connection |
| `SPRING_DATASOURCE_USERNAME` | `user` | DB username |
| `SPRING_DATASOURCE_PASSWORD` | `pass` | DB password |
| `SPRING_KAFKA_BOOTSTRAP_SERVERS` | `localhost:29092` | Kafka broker (host access) |
| `SPRING_DATA_REDIS_HOST` | `localhost` | Redis host |
| `GATEWAY_JWT_SECRET` | (dev placeholder) | HS256 JWT signing secret (min 32 chars) |
| `FRAUD_SERVICE_URL` | `http://localhost:8000` | Fraud service base URL |
| `REDIS_URL` | `redis://localhost:6379` | Redis URL for fraud service |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka for fraud service |
| `KAFKA_ENABLED` | `true` | Enable Kafka pipeline in fraud service |
| `ADMIN_API_KEY` | `changeme-admin-key-for-dev` | Fraud service admin endpoint auth |
| `VITE_DEV_JWT` | (empty) | JWT token for frontend API calls in dev |

### Installation

```bash
git clone <repository-url>
cd payments-system
```

## Run Instructions

### Option A — Docker Compose (Full Stack)

```bash
# Start all services (infrastructure + application)
docker compose --profile full up -d --build

# Wait for healthy status (~60-90s for first build)
docker compose ps

# Verify services
curl http://localhost:8080/actuator/health   # Payment Service
curl http://localhost:8090/actuator/health   # API Gateway
curl http://localhost:8000/health            # Fraud Service
```

| Service | URL |
|---------|-----|
| PostgreSQL | `localhost:5433` |
| Redis | `localhost:6379` |
| Kafka | `localhost:29092` |
| Fraud Service | http://localhost:8000 |
| Payment Service | http://localhost:8080 |
| API Gateway | http://localhost:8090 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 (admin/admin) |

```bash
# Stop services
docker compose --profile full down        # Keep data volumes
docker compose --profile full down -v     # Delete volumes (clean reset)
```

### Option B — Local Dev (Hot Reload)

Infrastructure runs in Docker; app services run locally.

**Terminal 1 — Infrastructure:**
```bash
docker compose up -d    # postgres, redis, kafka, zookeeper, prometheus, grafana
```

**Terminal 2 — Fraud Service:**
```bash
cd fraud-service
pip install -r requirements.txt
# Set env vars (PowerShell):
$env:REDIS_URL="redis://localhost:6379"; $env:KAFKA_BOOTSTRAP_SERVERS="localhost:29092"; $env:KAFKA_ENABLED="true"
# Or bash:
# export REDIS_URL=redis://localhost:6379 KAFKA_BOOTSTRAP_SERVERS=localhost:29092 KAFKA_ENABLED=true
python -m uvicorn main:app --host 0.0.0.0 --port 8000
```

**Terminal 3 — Payment Service:**
```bash
cd payment-service
mvn spring-boot:run
```

**Terminal 4 — API Gateway:**
```bash
cd api-gateway
mvn spring-boot:run
```

**Terminal 5 — Frontend:**
```bash
cd frontend
npm install
npm run dev    # → http://localhost:5173
```

## API Endpoints

### Payment Service (port 8080)

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/payments` | Create a new payment |
| `GET` | `/payments/{id}` | Get payment by ID |
| `GET` | `/payments?userId=&status=&page=&size=` | List payments (paginated, max 100) |
| `POST` | `/webhooks` | Register a webhook endpoint |

### API Gateway (port 8090)

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/v1/payments/**` | Versioned payment API (strips `/v1` prefix) |
| `*` | `/api/payments/**` | Legacy route (strips `/api` prefix) |
| `*` | `/api/webhooks/**` | Webhook registration |
| `GET` | `/management/**` | Actuator endpoints passthrough |

### Fraud Service (port 8000)

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/score` | Score a transaction for fraud |
| `GET` | `/health` | Health check |
| `GET` | `/metrics` | Prometheus metrics |
| `GET` | `/admin/model` | Model metadata (requires X-Admin-Key) |
| `POST` | `/admin/model/train-v2` | Train v2 model (requires X-Admin-Key) |

### Create Payment — Request/Response

**Request:**
```json
POST /payments
Headers: Idempotency-Key: <uuid>, Authorization: Bearer <jwt>

{
  "userId": "550e8400-e29b-41d4-a716-446655440000",
  "merchantId": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "amount": 150.00,
  "currency": "USD",
  "description": "Order #12345"
}
```

**Response:**
```json
{
  "paymentId": "a1b2c3d4-...",
  "userId": "550e8400-...",
  "merchantId": "7c9e6679-...",
  "amount": 150.00,
  "currency": "USD",
  "status": "PENDING",
  "idempotencyKey": "...",
  "description": "Order #12345",
  "createdAt": "2026-04-30T10:00:00"
}
```

## Features

- **Transactional Outbox Pattern** — Guarantees event delivery even if Kafka is temporarily down; events are written to the database in the same transaction as the payment
- **Exactly-Once Processing** — Idempotent consumer service with `processed_events` table prevents duplicate processing across Kafka redeliveries
- **Double-Entry Ledger** — Every payment creates balanced DEBIT + CREDIT entries; invariant enforced atomically
- **Two-Stage Fraud Detection** — Rule-based checks (velocity, threshold, currency) + ML scoring (IsolationForest) with model versioning
- **Idempotent API** — Same `Idempotency-Key` always returns the same response (Redis cache + DB-level pessimistic lock)
- **Adaptive Rate Limiting** — Redis-backed with Caffeine fallback; integrates with Kafka lag backpressure for dynamic throttling
- **Circuit Breaker** — Resilience4j circuit breaker on fraud service calls with automatic fallback
- **Dead Letter Queue** — Unprocessable messages routed to DLQ topics for manual review
- **Real-Time Dashboard** — WebSocket-powered frontend with live payment events, fraud alerts, and system metrics
- **Distributed Tracing** — Correlation ID + Trace ID propagated through HTTP → Kafka → all service boundaries
- **Webhook Notifications** — Register HTTPS endpoints for payment status callbacks with HMAC-SHA256 signing
- **Backpressure Monitoring** — Kafka consumer lag monitoring with adaptive ingestion throttling
- **Horizontal Scaling** — Composite partition keys (userId:salt) for even Kafka partition distribution
- **Observability** — Prometheus metrics, Grafana dashboards, structured JSON logging

## Folder Structure

```
payments-system/
├── api-gateway/             # Spring Cloud Gateway — JWT, rate limiting, routing
│   └── src/main/java/      #   SecurityConfig, RateLimiterConfig, filters
├── payment-service/         # Core payment processing service (Spring Boot)
│   └── src/main/java/
│       ├── config/          #   Kafka, Redis, WebSocket, WebClient configs
│       ├── controller/      #   REST endpoints (payments, webhooks)
│       ├── dto/             #   Request/response DTOs, Kafka event schemas
│       ├── entity/          #   JPA entities (Payment, Account, LedgerEntry, Outbox)
│       ├── exception/       #   Global exception handler, custom exceptions
│       ├── filter/          #   Correlation ID filter (MDC propagation)
│       ├── kafka/           #   Consumers (fraud result, payment events) + utilities
│       ├── metrics/         #   Prometheus metrics registry
│       ├── repository/      #   JPA repositories
│       ├── service/         #   Business logic (payment, ledger, outbox, rate limit)
│       └── websocket/       #   WebSocket handler, Redis pub/sub broadcaster
├── fraud-service/           # Python FastAPI ML fraud detection
│   ├── main.py             #   HTTP + Kafka endpoints, scoring logic
│   ├── model_manager.py    #   Versioned ML model lifecycle
│   ├── feature_engineering.py  # Feature extraction pipeline
│   └── feature_store.py    #   Redis-backed feature store
├── frontend/                # React + TypeScript dashboard
│   └── src/
│       ├── components/      #   UI components (forms, tables, alerts)
│       ├── hooks/           #   Custom hooks (WebSocket stream)
│       ├── pages/           #   Dashboard page
│       ├── services/        #   API client, WebSocket client
│       └── types/           #   TypeScript interfaces
├── docs/                    # Technical documentation
├── monitoring/              # Prometheus config + Grafana dashboards
├── load-tests/              # k6 load test scripts
├── scripts/                 # Failure simulation scripts
└── docker-compose.yml       # Full infrastructure + app orchestration
```

## Troubleshooting

| Problem | Solution |
|---------|----------|
| Kafka `NodeExistsException` on startup | Stale ZooKeeper data. Run `docker compose down -v` then start again |
| Port already in use | Kill the process: `netstat -ano \| findstr :<PORT>` (Windows) or `lsof -i :<PORT>` (macOS/Linux) |
| Payment Service fails to start | Ensure PostgreSQL is healthy: `docker compose ps` — wait for `healthy` status |
| Fraud Service can't connect to Kafka | Verify `KAFKA_BOOTSTRAP_SERVERS=localhost:29092` (host) or `kafka:9092` (Docker) |
| Frontend shows "API offline" | Start API Gateway, or check Vite proxy config targets correct ports |
| `scikit-learn` install fails | Requires Python 3.11+; use `pip install --upgrade pip` first |
| Redis connection refused | Ensure Redis container is running: `docker compose up redis -d` |
| Tests fail with connection errors | Tests exclude Kafka/Redis auto-config; ensure `src/test/resources/application.yml` is present |
| Grafana shows no data | Wait 30s for Prometheus scrape; verify targets at http://localhost:9090/targets |

## Documentation

| Topic | File |
|-------|------|
| [Architecture](docs/architecture.md) | System design, patterns, component interactions |
| [API Reference](docs/api.md) | Endpoints, schemas, error codes, Kafka topics |
| [Setup](docs/setup.md) | Prerequisites, configuration reference |
| [Running](docs/running.md) | Docker & local dev instructions |
| [Testing](docs/testing.md) | End-to-end test scenarios |
| [Failure Testing](docs/failure-testing.md) | Resilience tests (Kafka/Redis/DB down) |
| [Observability](docs/observability.md) | Logging, metrics, Grafana dashboards |
| [Performance](docs/performance.md) | Load testing, benchmarks, optimization |
| [Deep Dive](docs/deep-dive.md) | Interview-ready system design breakdown |

## Future Improvements

- **CQRS Read Model** — Separate read-optimized projections for payment queries (Elasticsearch or materialized views)
- **Saga Orchestrator** — Multi-step payment workflows (auth → capture → settle) with compensation
- **RS256 JWT** — Asymmetric key signing to eliminate shared secret between gateway and issuer
- **Sliding Window Rate Limiter** — Replace fixed-window Redis counter with sorted-set sliding window for smoother throttling
- **Schema Registry** — Avro/Protobuf schemas with Confluent Schema Registry for contract evolution
- **Multi-Region Deployment** — Active-active with CRDTs for idempotency keys and conflict-free ledger replication
- **Payment Method Abstraction** — Support cards, bank transfers, wallets via strategy pattern
- **Audit Trail Service** — Immutable append-only log for compliance and dispute resolution
- **A/B Testing for Fraud Models** — Shadow-mode model deployment with canary scoring
- **gRPC Inter-Service Communication** — Replace HTTP for internal service calls for lower latency
