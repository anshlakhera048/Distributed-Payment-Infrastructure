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

### Option A — Docker Only (simplest)

Run everything in Docker. No local Java/Python/Node needed.

```bash
# From project root (payments-system/)
docker compose --profile app up -d --build   # Start all backend services (~2-4 min first build)
docker compose ps                            # Verify healthy status (~60-90s)
```

| # | Service | URL |
|---|---------|-----|
| 1 | PostgreSQL | `localhost:5433` |
| 2 | Redis | `localhost:6379` |
| 3 | Kafka | `localhost:29092` |
| 4 | Fraud Service | http://localhost:8000 |
| 5 | Payment Service | http://localhost:8080 |
| 6 | API Gateway | http://localhost:8090 |
| 7 | Prometheus | http://localhost:9090 |
| 8 | Grafana | http://localhost:3000 (admin/admin) |

Frontend: `cd frontend && npm install && npm run dev` → http://localhost:5173

To stop:
```bash
docker compose --profile app down              # Stop all, keep data
docker compose --profile app down -v           # Stop all + delete volumes (clean reset)
```

### Option B — Local Dev (hot-reload for app services)

Infrastructure runs in Docker, app services run locally for fast iteration.

**Prerequisites:** Java 17+, Python 3.11+, Node 18+, Maven

**Step 1 — Start infrastructure (one terminal, from project root):**
```bash
docker compose up -d                           # Starts only: postgres, redis, kafka, zookeeper, prometheus, grafana
```

**Step 2 — Start Fraud Service (new terminal):**
```bash
cd fraud-service

# Activate virtual environment:
#   Windows PowerShell:
.\venv\Scripts\Activate.ps1
#   Linux/macOS:
#   source venv/bin/activate

pip install -r requirements.txt

# Set environment variables:
#   Windows PowerShell:
$env:REDIS_URL="redis://localhost:6379"; $env:KAFKA_BOOTSTRAP_SERVERS="localhost:29092"; $env:KAFKA_ENABLED="true"
#   Linux/macOS:
#   export REDIS_URL=redis://localhost:6379 KAFKA_BOOTSTRAP_SERVERS=localhost:29092 KAFKA_ENABLED=true

python -m uvicorn main:app --host 0.0.0.0 --port 8000
```

**Step 3 — Start Payment Service (new terminal):**
```bash
cd payment-service
mvn spring-boot:run
```

**Step 4 — Start API Gateway (new terminal):**
```bash
cd api-gateway
mvn spring-boot:run
```

**Step 5 — Start Frontend (new terminal):**
```bash
cd frontend
npm install
npm run dev
```

> **Port conflict?** If you see "Port already in use", either a Docker container or a previous local process is occupying the port. Run `netstat -ano | findstr :<PORT>` (Windows) or `lsof -i :<PORT>` (macOS/Linux) to find the PID and kill it.

### Troubleshooting

| Problem | Fix |
|---------|-----|
| Kafka `NodeExistsException` on startup | Stale ZooKeeper data. Run `docker-compose down -v` then start again |
| Port already in use | Kill the conflicting process (see note above) or stop Docker app containers |
| `scikit-learn` or `confluent-kafka` fails to install | Requires Python 3.11+; on Python 3.13+ the versions in `requirements.txt` are already updated |
| Grafana occupies port 3000 | Frontend uses port 5173 to avoid conflict |

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
