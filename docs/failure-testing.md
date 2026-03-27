# Failure Testing

Scenarios that validate the system's resilience when infrastructure components fail. Each test documents the expected degradation behavior and recovery path.

---

## Kafka Down

```bash
docker compose stop kafka

# Create a payment — succeeds (writes to DB + outbox)
curl -s -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: kafka-down-001" \
  -d '{"userId":"550e8400-e29b-41d4-a716-446655440000","merchantId":"660e8400-e29b-41d4-a716-446655440000","amount":100,"currency":"USD","description":"Kafka down test"}'
# → 201 Created

# Outbox events stay NEW, retry_count increments
docker exec -it payments-postgres psql -U user payments \
  -c "SELECT event_type, status, retry_count FROM outbox_events ORDER BY created_at DESC LIMIT 3;"

# Recover
docker compose start kafka
sleep 15

# Outbox poller re-publishes within seconds; payment completes normally
```

**Behavior:**
- `POST /payments` continues to work (DB write succeeds)
- Outbox events accumulate as `NEW` with incrementing `retry_count`
- After Kafka recovers, backlog publishes within seconds
- No data loss, no duplicates

---

## Redis Down

```bash
docker compose stop redis

# Rate limiter falls back to Caffeine in-memory cache
curl -s -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: redis-down-001" \
  -d '{"userId":"550e8400-e29b-41d4-a716-446655440000","merchantId":"660e8400-e29b-41d4-a716-446655440000","amount":100,"currency":"USD","description":"Redis down test"}'
# → 201 Created

# Check logs for fallback activation
docker compose logs payment-service | grep "Redis unavailable"

# Recover
docker compose start redis
```

**Behavior:**
- Rate limiting degrades to per-JVM Caffeine cache (50K entries, 2-min TTL)
- Idempotency cache misses → falls back to DB pessimistic lock
- Payment cache misses → falls back to DB read
- WebSocket pub/sub paused until Redis recovers

---

## Fraud Service Down

```bash
docker compose stop fraud-service

curl -s -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: fraud-down-001" \
  -d '{"userId":"550e8400-e29b-41d4-a716-446655440000","merchantId":"660e8400-e29b-41d4-a716-446655440000","amount":100,"currency":"USD","description":"Fraud down test"}'
# → 201 Created, status=PENDING

# Payment stays PENDING (fraud.request events queue in Kafka)
curl -s http://localhost:8080/payments/<paymentId> | python -m json.tool

# Recover — fraud service consumes the backlog (3 parallel workers)
docker compose start fraud-service
sleep 15
curl -s http://localhost:8080/payments/<paymentId> | python -m json.tool
# → status=SUCCESS
```

**Behavior:**
- Payments are accepted and saved as `PENDING`
- `fraud.request` events queue in Kafka (retained 168 hours)
- On recovery, backlog is consumed in parallel
- No data loss, no double-processing

---

## PostgreSQL Down

```bash
docker compose stop postgres

# Payment creation fails
curl -s -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: db-down-001" \
  -d '{"userId":"550e8400-e29b-41d4-a716-446655440000","merchantId":"660e8400-e29b-41d4-a716-446655440000","amount":100,"currency":"USD","description":"DB down test"}'
# → 500 Internal Server Error

# Recover — HikariCP reconnects automatically within 30 seconds
docker compose start postgres
sleep 10

curl -s -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: db-recovery-001" \
  -d '{"userId":"550e8400-e29b-41d4-a716-446655440000","merchantId":"660e8400-e29b-41d4-a716-446655440000","amount":100,"currency":"USD","description":"DB recovery test"}'
# → 201 Created
```

---

## Consumer Restart (Idempotency Test)

```bash
# Create multiple payments
for i in $(seq 1 5); do
  curl -s -X POST http://localhost:8080/payments \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: restart-test-$i" \
    -d "{\"userId\":\"550e8400-e29b-41d4-a716-446655440000\",\"merchantId\":\"660e8400-e29b-41d4-a716-446655440000\",\"amount\":$((i * 50)),\"currency\":\"USD\",\"description\":\"Restart test $i\"}"
done

# Immediately restart the payment-service container
docker compose restart payment-service
sleep 30

# All payments reach terminal state (no stuck PENDING)
docker exec -it payments-postgres psql -U user payments \
  -c "SELECT status, COUNT(*) FROM payments WHERE idempotency_key LIKE 'restart-test-%' GROUP BY status;"

# No duplicate ledger entries
docker exec -it payments-postgres psql -U user payments \
  -c "SELECT payment_id, COUNT(*) FROM ledger_entries GROUP BY payment_id HAVING COUNT(*) > 2;"
# → 0 rows
```
