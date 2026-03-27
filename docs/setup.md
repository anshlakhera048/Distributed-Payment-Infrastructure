# Setup

## Prerequisites

| Tool | Required | Verify |
|------|----------|--------|
| Docker Desktop | Yes | `docker --version` |
| Docker Compose v2 | Yes | `docker compose version` |
| Java 17 | Local dev only | `java -version` |
| Python 3.11+ | Local dev only | `python --version` |
| k6 | Load testing only | `k6 version` |

> **Fully containerised** — `docker compose up --build` runs everything. Java/Python are only needed for running services outside Docker.

## Ports

| Port | Service |
|------|---------|
| 3000 | Grafana |
| 5433 | PostgreSQL |
| 6379 | Redis |
| 8000 | Fraud Service |
| 8080 | Payment Service |
| 8090 | API Gateway |
| 9090 | Prometheus |
| 29092 | Kafka (host access) |

The frontend Vite dev server auto-picks an available port (default 3000, increments if occupied).

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

## Database

| Parameter | Value |
|-----------|-------|
| Host | `localhost` |
| Port | `5433` |
| Database | `payments` |
| Username | `user` |
| Password | `pass` |

Connect via CLI:

```bash
docker exec -it payments-postgres psql -U user payments
```
