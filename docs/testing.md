# Testing Guide

All test scenarios assume the system is running via `docker compose up --build`. Wait until `payments-payment-service` shows `healthy` status before testing.

---

## Normal Payment Flow

**Services:** Payment Service, PostgreSQL, Kafka, Redis, Fraud Service

```bash
# Create a payment
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

**Expected:** Status `PENDING` immediately. After 3-5 seconds, poll with `GET /payments/<paymentId>` — status becomes `SUCCESS`.

**Validate:**

```bash
# Check DB
docker exec -it payments-postgres psql -U user payments \
  -c "SELECT id, status, fraud_score FROM payments WHERE idempotency_key = 'test-normal-001';"

# Check ledger (1 DEBIT + 1 CREDIT, both 49.99)
docker exec -it payments-postgres psql -U user payments \
  -c "SELECT entry_type, amount, account_id FROM ledger_entries WHERE payment_id = '<paymentId>';"

# Check outbox (payment.created=PUBLISHED, payment.processed=PUBLISHED)
docker exec -it payments-postgres psql -U user payments \
  -c "SELECT event_type, status FROM outbox_events WHERE aggregate_id = '<paymentId>';"
```

---

## Idempotent Payment Replay

**Services:** Payment Service, PostgreSQL, Redis

```bash
KEY="idem-replay-$(date +%s)"

# First request — creates the payment
curl -s -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $KEY" \
  -d '{"userId":"550e8400-e29b-41d4-a716-446655440000","merchantId":"660e8400-e29b-41d4-a716-446655440000","amount":100,"currency":"USD","description":"Idem test"}' \
  | python -m json.tool

# Replay with the SAME idempotency key
curl -s -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $KEY" \
  -d '{"userId":"550e8400-e29b-41d4-a716-446655440000","merchantId":"660e8400-e29b-41d4-a716-446655440000","amount":100,"currency":"USD","description":"Idem test"}' \
  | python -m json.tool
```

**Expected:** Both return the **same paymentId** with HTTP 201. Only one row in the database.

---

## Fraud Rejection Flow

**Services:** Payment Service, PostgreSQL, Kafka, Fraud Service

```bash
# High-value payment (>$10,000 triggers rule-based fraud rejection)
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

**Expected:** `PENDING` initially → `FRAUD_REJECTED` after 3-5 seconds, `fraudScore=0.95`, `failureReason="FRAUD: amount_exceeds_threshold"`. No ledger entries created. Outbox contains `payment.failed` event.

---

## Unsupported Currency Rejection

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

**Expected:** After 3-5 seconds, `FRAUD_REJECTED` with reason `"unsupported_currency"`.

---

## Ledger Consistency Validation

```bash
# Create several successful payments
for i in $(seq 1 5); do
  curl -s -X POST http://localhost:8080/payments \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: ledger-test-$i" \
    -d "{\"userId\":\"550e8400-e29b-41d4-a716-446655440000\",\"merchantId\":\"660e8400-e29b-41d4-a716-446655440000\",\"amount\":$((i * 100)),\"currency\":\"USD\",\"description\":\"Ledger test $i\"}"
  sleep 1
done
sleep 10
```

**Validate:**

```sql
-- Every SUCCESS payment must have exactly 2 ledger entries (0 rows = consistent)
SELECT p.id, p.status, COUNT(l.id) AS ledger_count
FROM payments p LEFT JOIN ledger_entries l ON l.payment_id = p.id
WHERE p.status = 'SUCCESS' GROUP BY p.id, p.status HAVING COUNT(l.id) != 2;

-- Debits must equal credits globally (0 rows = balanced)
SELECT payment_id,
       SUM(CASE WHEN entry_type = 'DEBIT' THEN amount ELSE 0 END) AS debits,
       SUM(CASE WHEN entry_type = 'CREDIT' THEN amount ELSE 0 END) AS credits
FROM ledger_entries GROUP BY payment_id
HAVING SUM(CASE WHEN entry_type = 'DEBIT' THEN amount ELSE 0 END) !=
       SUM(CASE WHEN entry_type = 'CREDIT' THEN amount ELSE 0 END);
```

---

## Kafka Duplicate Event Handling

Tests the exactly-once processing guarantee via `IdempotentConsumerService`.

```bash
# Create a payment and wait for completion
curl -s -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: dedup-test-001" \
  -d '{"userId":"550e8400-e29b-41d4-a716-446655440000","merchantId":"660e8400-e29b-41d4-a716-446655440000","amount":200,"currency":"USD","description":"Dedup test"}'
sleep 10
```

**Validate:**

```sql
-- Each eventId appears exactly once (0 rows = no duplicates)
SELECT event_id, COUNT(*) FROM processed_events GROUP BY event_id HAVING COUNT(*) > 1;

-- No double-writes to ledger (0 rows = clean)
SELECT payment_id, COUNT(*) FROM ledger_entries GROUP BY payment_id HAVING COUNT(*) > 2;
```

---

## Outbox Replay Scenario

Tests event delivery recovery when Kafka is temporarily unavailable.

```bash
# Stop Kafka
docker compose stop kafka

# Create a payment (DB write succeeds, Kafka publish will fail)
curl -s -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: outbox-test-001" \
  -d '{"userId":"550e8400-e29b-41d4-a716-446655440000","merchantId":"660e8400-e29b-41d4-a716-446655440000","amount":300,"currency":"USD","description":"Outbox test"}'

# Outbox event should be NEW (not yet published)
docker exec -it payments-postgres psql -U user payments \
  -c "SELECT event_type, status, retry_count FROM outbox_events WHERE aggregate_id IN (SELECT id::text FROM payments WHERE idempotency_key = 'outbox-test-001');"

# Restart Kafka — outbox poller re-publishes within seconds
docker compose start kafka
sleep 15

# Event should now be PUBLISHED, payment eventually reaches SUCCESS
```

---

## Rate Limiting

**Services:** Payment Service, Redis

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

**Expected:** First ~100 requests return `201`. After the limit, requests return `429 Too Many Requests`.

---

## Trace ID / Correlation ID Propagation

```bash
curl -s -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: trace-test-001" \
  -H "X-Trace-ID: my-trace-id-12345" \
  -H "X-Correlation-ID: my-correlation-id-67890" \
  -d '{"userId":"550e8400-e29b-41d4-a716-446655440000","merchantId":"660e8400-e29b-41d4-a716-446655440000","amount":50,"currency":"USD","description":"Trace test"}' \
  -v 2>&1 | grep -i "x-trace\|x-correlation"
```

**Expected:** Response headers contain the same `X-Trace-ID` and `X-Correlation-ID`. Values are persisted in the `payments` and `outbox_events` tables and propagated through Kafka headers.

---

## Webhook Registration and Delivery

```bash
# Register a webhook endpoint
curl -s -X POST http://localhost:8080/webhooks \
  -H "Content-Type: application/json" \
  -d '{
    "merchantId": "660e8400-e29b-41d4-a716-446655440000",
    "url": "https://httpbin.org/post",
    "events": "payment.processed,payment.failed"
  }' | python -m json.tool

# Create a payment for that merchant
curl -s -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: webhook-test-001" \
  -d '{"userId":"550e8400-e29b-41d4-a716-446655440000","merchantId":"660e8400-e29b-41d4-a716-446655440000","amount":75,"currency":"USD","description":"Webhook test"}'

# Check webhook deliveries
sleep 10
docker exec -it payments-postgres psql -U user payments \
  -c "SELECT event_type, status, attempt_count, last_response_code FROM webhook_deliveries ORDER BY created_at DESC LIMIT 5;"
```

---

## DLQ Routing

```bash
# Produce a malformed message to fraud.result
docker exec payments-kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic fraud.result <<< '{"invalid": "json without required fields"}'

sleep 5

# Verify it was routed to the DLQ
docker exec payments-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic fraud.result.DLQ \
  --from-beginning --max-messages 1 --timeout-ms 5000
```

---

## Kafka Topic Inspection

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

## WebSocket Real-Time Updates

```bash
# 1. Connect to WebSocket (requires wscat: npm install -g wscat)
wscat -c ws://localhost:8080/ws/payments

# 2. In another terminal, create a payment
curl -s -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: ws-test-001" \
  -d '{"userId":"550e8400-e29b-41d4-a716-446655440000","merchantId":"660e8400-e29b-41d4-a716-446655440000","amount":50,"currency":"USD","description":"WebSocket test"}'

# 3. Observe: WebSocket receives a JSON message with the payment.processed event within ~3-5 seconds
```

---

## Reconciliation Service

```bash
# Stop fraud-service so payments stay PENDING
docker compose stop fraud-service

# Create a payment
curl -s -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: recon-test-001" \
  -d '{"userId":"550e8400-e29b-41d4-a716-446655440000","merchantId":"660e8400-e29b-41d4-a716-446655440000","amount":50,"currency":"USD","description":"Reconciliation test"}'

# Reconciliation runs every 30 min — check logs:
docker compose logs payment-service | grep "RECONCILIATION"
# Expected: STALE_PAYMENT alert after stale-payment-threshold-minutes (10 min)

# Restart fraud-service
docker compose start fraud-service
```

---

## Load Testing

### Install k6

```bash
# macOS
brew install k6

# Windows
choco install k6
```

### Run

```bash
# Default profile (direct to payment-service)
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
| Ramp-up | 0–30s | 0 → 10 | Warm caches, JIT |
| Sustained | 30–90s | 10 → 50 | Steady-state performance |
| Spike | 90–120s | 50 → 100 | Stress test |
| Scale down | 120–150s | 100 → 10 | Recovery validation |
| Cool down | 150–160s | 10 → 0 | Clean shutdown |

### Thresholds

| Metric | Threshold | Description |
|--------|-----------|-------------|
| `payment_create_duration` | p(95) < 500ms | POST /payments latency |
| `payment_get_duration` | p(95) < 200ms | GET /payments/:id (cache hit) |
| `http_req_failed` | rate < 0.01 | Error rate (429s excluded) |
| `idempotency_check_pass` | rate > 0.99 | Idempotency replay success |
