# Running the System

## Option A: Full Docker (Recommended)

```bash
cd payments-system

# Start all services (first build takes ~2-4 min for Maven downloads)
docker compose up --build

# In a separate terminal, watch logs
docker compose logs -f payment-service fraud-service

# Verify all containers are healthy (~60-90s after build)
docker compose ps
```

Wait until `payments-payment-service` shows `healthy` status.

## Option B: Local Development (Hybrid)

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

## Frontend Dashboard

```bash
cd frontend
npm install
npm run dev
```

The Vite dev server starts on an available port (default 3000). API calls proxy to the gateway at `localhost:8090`.

### Dashboard Panels

| Panel | Description |
|---|---|
| **Metrics Bar** | Total, Success, Pending, Failed, Fraud counts + success rate |
| **Live Payment Stream** | Real-time table of events via WebSocket |
| **System Status** | Health indicators for API Gateway and WebSocket connectivity |
| **Create Payment** | Form to submit payments with instant response display |
| **Event Stream** | Kafka-style timeline of payment lifecycle events |
| **Alerts & Fraud** | Fraud rejections and failed payment alerts |

### Simulation Controls

- **Duplicate Request** — Resends with the same idempotency key to demonstrate exactly-once semantics
- **Burst (5 rapid)** — Fires 5 concurrent payments to test parallel processing and rate limiting

## Health Checks

```bash
# Payment service
curl http://localhost:8080/actuator/health

# Fraud service
curl http://localhost:8000/health

# API Gateway
curl http://localhost:8090/actuator/health
```

## Shutdown

```bash
docker compose down          # Stop (data preserved in volumes)
docker compose down -v       # Stop + wipe all persistent data
```

## Useful SQL Queries

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

-- Verify ledger invariant (should always return 0 rows)
SELECT payment_id,
       SUM(CASE WHEN entry_type = 'DEBIT' THEN amount ELSE 0 END) AS debits,
       SUM(CASE WHEN entry_type = 'CREDIT' THEN amount ELSE 0 END) AS credits
FROM ledger_entries GROUP BY payment_id
HAVING SUM(CASE WHEN entry_type = 'DEBIT' THEN amount ELSE 0 END) !=
       SUM(CASE WHEN entry_type = 'CREDIT' THEN amount ELSE 0 END);

-- Webhook endpoints registered
SELECT id, merchant_id, url, events, active FROM webhook_endpoints;

-- Webhook delivery status
SELECT id, payment_id, event_type, status, attempt_count, last_error
FROM webhook_deliveries ORDER BY created_at DESC LIMIT 20;
```
