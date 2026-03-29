# Failure Scenarios & Recovery Guide

## Table of Contents
1. [Failure Classification](#failure-classification)
2. [Infrastructure Failures](#infrastructure-failures)
3. [Application Failures](#application-failures)
4. [Data Consistency Failures](#data-consistency-failures)
5. [Failure Simulation Scripts](#failure-simulation-scripts)
6. [Recovery Procedures](#recovery-procedures)
7. [Monitoring & Alerting](#monitoring--alerting)

---

## Failure Classification

| Category | Impact | Recovery | Example |
|----------|--------|----------|---------|
| **Transient** | Self-healing | Automatic retry | Network blip, brief DB pause |
| **Degraded** | Reduced throughput | Fallback paths | Redis down, high Kafka lag |
| **Critical** | Service unavailable | Manual intervention | DB corruption, Kafka topic loss |
| **Cascading** | Multi-service impact | Coordinated recovery | DB pool exhaustion → Kafka lag → backpressure |

---

## Infrastructure Failures

### 1. PostgreSQL Failure

**Symptom:** 500 errors on all write operations  
**Impact:** CRITICAL — DB is in the critical path for payment creation  
**Detection:**
- `hikaricp_connections_active` drops to 0
- Health check `/actuator/health` reports DOWN
- Kafka consumer processing fails (TransientDataAccessException)

**Automatic Recovery:**
- HikariCP connection pool reconnects after DB restarts
- Spring Retry retries failed consumer operations (3 attempts, 200ms backoff)
- Reconciliation service detects stale PENDING payments on recovery

**Manual Recovery:**
1. Verify PostgreSQL is running: `docker compose ps postgres`
2. Check HikariCP pool: `curl localhost:8080/actuator/health | jq .components.db`
3. Monitor stale payments: Run reconciliation manually via cron trigger

**Data Safety:** All operations are @Transactional. No partial writes possible.

---

### 2. Kafka Broker Failure

**Symptom:** Outbox events stuck in NEW status, consumer lag stops updating  
**Impact:** DEGRADED — Payments can still be created (outbox pattern), but async processing halts  
**Detection:**
- `kafka_consumer_lag_total` stops changing (stale metric)
- `outbox_events_failed_total` increases
- Payment status stuck at PENDING

**Automatic Recovery:**
- Outbox publisher retries every 1s (up to `max-retries: 3`)
- Kafka producer retries (5 retries, 120s delivery timeout)
- Consumer reconnects automatically on broker recovery
- Backpressure activates to slow ingestion during catch-up

**Manual Recovery:**
1. Check Kafka: `docker compose logs kafka | tail -50`
2. Verify topics: `docker compose exec kafka kafka-topics --list --bootstrap-server localhost:9092`
3. Check consumer groups: `docker compose exec kafka kafka-consumer-groups --group payment-processor-group --describe --bootstrap-server localhost:9092`

**Data Safety:** Outbox pattern guarantees no event loss. Events remain in `outbox_events` table until published.

---

### 3. Redis Failure

**Symptom:** Fallback log messages, slightly higher latency  
**Impact:** DEGRADED — All Redis-dependent features have fallbacks  
**Detection:**
- Logs: `"Redis unavailable for rate limiting... using in-memory fallback"`
- `lettuce_command_firstresponse_seconds` metric disappears
- Health check shows redis component as DOWN

**Automatic Recovery:**
- Rate limiter: Caffeine in-memory cache (50K entries, 2min TTL)
- Payment cache: DB fallback (cache miss → SELECT)
- Idempotency: DB `SELECT FOR UPDATE` (Redis is optimization layer)
- Fraud velocity: In-process deque fallback

**Trade-offs During Redis Outage:**
| Feature | Normal | Degraded |
|---------|--------|----------|
| Rate limiting | Cluster-wide (shared via Redis) | Per-pod (each pod has own limit) |
| Idempotency | O(1) Redis lookup | O(log n) DB index scan |
| Payment cache | Redis cache hit | DB query every time |
| Fraud velocity | Cross-instance (Redis) | Per-process (in-memory deque) |

**Data Safety:** No data loss. Redis is never the source of truth.

---

### 4. Fraud Service Failure

**Symptom:** Payments stuck in PENDING, fraud.request events accumulating  
**Impact:** DEGRADED — Payment creation works, but no fraud decisions  
**Detection:**
- `kafka_consumer_lag_total` on `fraud.request` topic increasing
- Resilience4j circuit breaker metrics show OPEN state
- No `payments_processed_total` counter increments

**Automatic Recovery:**
- Kafka retains events (default 7-day retention)
- Fraud service catches up on queued events after restart
- Reconciliation service flags stale PENDING payments after threshold

**Manual Recovery:**
1. Check fraud service: `curl localhost:8000/health`
2. Restart: `docker compose restart fraud-service`
3. Monitor catch-up: Watch `kafka_consumer_lag_total`

---

## Application Failures

### 5. Optimistic Lock Conflict (Account Balance)

**Symptom:** `OptimisticLockException` in logs during high-concurrency payments  
**Impact:** TRANSIENT — Automatic retry handles it  
**Detection:**
- `jakarta.persistence.OptimisticLockException` in logs
- Slightly elevated p99 latency

**Automatic Recovery:**
- Spring Retry retries the entire `processPaymentResult` transaction
- 3 attempts with 200ms × 2 exponential backoff
- After exhaustion: Kafka consumer error handler retries (1s → 2s → 4s → 8s)
- After all retries exhausted: event routed to DLQ

**Root Cause:** Two payments for the same user processed simultaneously, both trying to update the same Account row.

---

### 6. Ledger Invariant Violation

**Symptom:** `LEDGER INVARIANT VIOLATION` in logs  
**Impact:** CRITICAL — Indicates a bug in ledger logic  
**Detection:**
- IllegalStateException with "LEDGER INVARIANT VIOLATION" message
- Payment status remains PENDING (transaction rolled back)

**Recovery:**
1. This should NEVER happen in normal operation
2. Check the specific paymentId in logs
3. Inspect ledger_entries table: `SELECT * FROM ledger_entries WHERE payment_id = ?`
4. Verify SUM(DEBIT) = SUM(CREDIT) manually
5. File a bug report with the full stack trace

**Data Safety:** Transaction rollback prevents any inconsistent state from persisting.

---

### 7. Outbox Event Exhaustion

**Symptom:** Events stuck in FAILED status in outbox_events table  
**Impact:** DEGRADED — Specific payments don't get Kafka events  
**Detection:**
- `outbox_events_failed_total` counter > 0
- `SELECT COUNT(*) FROM outbox_events WHERE status = 'FAILED'`

**Manual Recovery:**
1. Identify failed events: `SELECT * FROM outbox_events WHERE status = 'FAILED' ORDER BY created_at`
2. Check `last_error` column for root cause
3. Fix root cause (usually Kafka connectivity)
4. Reset for retry: `UPDATE outbox_events SET status = 'NEW', retry_count = 0 WHERE status = 'FAILED'`

---

### 8. Duplicate Payment Processing

**Symptom:** Same paymentId appears twice in logs  
**Impact:** NONE — Idempotency guard prevents duplicates  
**Detection:**
- Logs: `"Duplicate event skipped — eventId=..."`
- `processed_events` table has the event recorded

**Explanation:** This is expected behavior during Kafka consumer rebalances or retry scenarios. The three-layer dedup (processed_events table → payment status guard → Kafka consumer ack) ensures exactly-once processing semantics.

---

## Data Consistency Failures

### 9. Stale PENDING Payments

**Symptom:** Payments stuck in PENDING beyond threshold (default: 10 minutes)  
**Impact:** DEGRADED — Customer sees payment as "processing" indefinitely  
**Detection:**
- Reconciliation service runs every 30 minutes
- Publishes to `reconciliation.alerts` topic
- `SELECT * FROM payments WHERE status = 'PENDING' AND created_at < NOW() - INTERVAL '10 minutes'`

**Recovery:**
1. Check if fraud.request was published (outbox_events table)
2. Check if fraud.result was received (processed_events table)
3. If fraud.request never published: outbox publisher issue → restart
4. If fraud.result never received: fraud service issue → check fraud service logs
5. Manual resolution: Update payment status based on investigation

### 10. Account Balance Drift

**Symptom:** Sum of ledger entries doesn't match account balance  
**Detection:**
```sql
SELECT a.owner_id, a.currency, a.balance as account_balance,
       COALESCE(SUM(CASE WHEN le.entry_type = 'CREDIT' THEN le.amount ELSE 0 END), 0) -
       COALESCE(SUM(CASE WHEN le.entry_type = 'DEBIT' THEN le.amount ELSE 0 END), 0) as ledger_balance
FROM accounts a
LEFT JOIN ledger_entries le ON a.owner_id = le.account_id
GROUP BY a.owner_id, a.currency, a.balance
HAVING a.balance != COALESCE(SUM(CASE WHEN le.entry_type = 'CREDIT' THEN le.amount ELSE 0 END), 0) -
                     COALESCE(SUM(CASE WHEN le.entry_type = 'DEBIT' THEN le.amount ELSE 0 END), 0);
```
**Recovery:** Should never happen (ledger and account updates are in the same transaction). If detected, indicates a bug requiring investigation.

---

## Failure Simulation Scripts

Located in `scripts/failure-simulation/`:

| Script | What It Tests | Duration |
|--------|--------------|----------|
| `kafka-failure.sh` | Kafka broker pause/kill | 30s default |
| `redis-failure.sh` | Redis unavailability | 30s default |
| `database-failure.sh` | PostgreSQL pause | 15s default |
| `fraud-service-failure.sh` | Fraud service pause | 30s default |
| `network-partition.sh` | Network isolation | 20s default |

### Running Simulations
```bash
cd scripts/failure-simulation

# Basic usage
./kafka-failure.sh           # Pause Kafka for 30s
./redis-failure.sh 60        # Pause Redis for 60s
./database-failure.sh        # Pause PostgreSQL for 15s
./fraud-service-failure.sh   # Pause fraud service for 30s
./network-partition.sh fraud-service 20  # Isolate fraud service for 20s

# Combined with load testing
# Terminal 1: Start load test
k6 run --env PROFILE=stress load-tests/payment-load-test.js
# Terminal 2: Inject failure
./kafka-failure.sh 30
```

---

## Recovery Procedures

### Full System Recovery (After Total Outage)

1. **Start infrastructure first:**
   ```bash
   docker compose up -d postgres redis zookeeper kafka
   ```
2. **Wait for health checks** (30s)
3. **Start services:**
   ```bash
   docker compose up -d payment-service fraud-service api-gateway
   ```
4. **Verify health:**
   ```bash
   curl localhost:8080/actuator/health
   curl localhost:8000/health
   curl localhost:8090/actuator/health
   ```
5. **Check Kafka consumer groups:**
   ```bash
   docker compose exec kafka kafka-consumer-groups \
     --group payment-processor-group \
     --describe \
     --bootstrap-server localhost:9092
   ```
6. **Monitor recovery:** Watch Grafana dashboards for lag recovery

---

## Monitoring & Alerting

### Critical Alerts (Page)
| Metric | Threshold | Action |
|--------|-----------|--------|
| `up{job="payment-service"}` | == 0 for 1m | Service down, restart |
| `hikaricp_connections_active` | == max_pool for 5m | DB pool exhaustion |
| `kafka_consumer_lag_total` | > 10,000 for 5m | Consumer can't keep up |
| `payments_created_total` - rate | == 0 for 5m | No payments being created |

### Warning Alerts (Ticket)
| Metric | Threshold | Action |
|--------|-----------|--------|
| `kafka_consumer_lag_total` | > 1,000 for 10m | Investigate lag |
| `outbox_events_failed_total` | > 0 | Check failed events |
| `http_server_requests_seconds{quantile="0.99"}` | > 2s | Performance degradation |
| `fraud_detected_total` rate | > 10% of total | Unusual fraud spike |
