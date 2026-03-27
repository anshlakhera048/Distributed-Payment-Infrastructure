# Distributed Payments System

A production-grade distributed payments platform demonstrating event-driven microservices, the Transactional Outbox Pattern, real-time ML fraud detection, and exactly-once processing semantics — all running on Docker Compose.

## Architecture

```
Client → API Gateway (JWT, rate limit) → Payment Service (Spring Boot)
                                              ├── PostgreSQL (payments, ledger, outbox)
                                              ├── Redis (cache, rate limit, WebSocket pub/sub)
                                              └── Kafka ←→ Fraud Service (FastAPI + ML)
```

**Services:** Payment Service (Java 17 / Spring Boot) · Fraud Service (Python 3.11 / FastAPI) · API Gateway (Spring Cloud Gateway) · PostgreSQL · Kafka · Redis · Prometheus · Grafana

**Key patterns:** Transactional Outbox · Exactly-Once Consumers · Double-Entry Ledger · Circuit Breaker · Idempotent API · DLQ Routing · Distributed Tracing

## Quick Start

```bash
docker compose up --build     # Start all services (~2-4 min first build)
docker compose ps             # Verify healthy status (~60-90s)
```

Create a test payment:

```bash
curl -s -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: test-001" \
  -d '{
    "userId": "550e8400-e29b-41d4-a716-446655440000",
    "merchantId": "660e8400-e29b-41d4-a716-446655440000",
    "amount": 49.99,
    "currency": "USD"
  }' | python -m json.tool
```

Frontend dashboard: `cd frontend && npm install && npm run dev`

## Documentation

| Topic | Description |
|-------|-------------|
| [Architecture](docs/architecture.md) | System overview, design patterns, service components, architecture diagram |
| [Setup](docs/setup.md) | Prerequisites, ports, configuration reference, database connection |
| [Running](docs/running.md) | Docker & local dev instructions, health checks, frontend dashboard, SQL queries |
| [Testing](docs/testing.md) | End-to-end test scenarios, idempotency, fraud, ledger, webhooks, load testing |
| [Failure Testing](docs/failure-testing.md) | Resilience tests: Kafka/Redis/DB/Fraud down, consumer restart |
| [Observability](docs/observability.md) | Structured logging, Prometheus metrics, Grafana dashboards, troubleshooting |
| [API Reference](docs/api.md) | Endpoints, request/response formats, error codes, Kafka topics, WebSocket |
