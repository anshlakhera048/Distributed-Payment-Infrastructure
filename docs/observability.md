# Observability

## Structured Logging

All logs include `traceId` and `correlationId` via SLF4J MDC:

```json
{
  "@timestamp": "...",
  "level": "INFO",
  "traceId": "abc123",
  "correlationId": "def456",
  "message": "Payment created: paymentId=xyz789"
}
```

```bash
docker compose logs -f payment-service
docker compose logs -f fraud-service
```

## Prometheus Metrics

**URL:** http://localhost:9090

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

Prometheus scrapes both services every 15 seconds:
- `payment-service:8080/actuator/prometheus`
- `fraud-service:8000/metrics`

## Grafana Dashboards

1. Open http://localhost:3000 (admin / admin)
2. Add data source → Prometheus → URL: `http://prometheus:9090`
3. Import dashboards:
   - JVM (Micrometer): ID `4701`
   - Spring Boot: ID `12900`
   - Kafka: ID `7589`

### What to Monitor During Load

- **Prometheus:** `payments_created_total`, `fraud_detected_total`
- **Grafana:** JVM heap, Kafka consumer lag, p95 latencies
- **Logs:** `docker compose logs -f payment-service` — look for rate limit warnings, Kafka errors
- **DB pool:** `SELECT count(*) FROM pg_stat_activity WHERE datname = 'payments';`

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| `payment-service` never becomes healthy | Maven downloading deps (~2-4 min) | Wait; check `docker compose logs payment-service` |
| `Connection refused` to Kafka from host | Kafka external listener is on port 29092 | Use `localhost:29092` from host machine |
| Payments stuck in `PENDING` | Fraud service not healthy or Kafka backlog | `docker compose ps fraud-service`; wait or restart |
| `401 Unauthorized` from API Gateway | Missing or expired JWT | Generate new HS256 JWT with `GATEWAY_JWT_SECRET` |
| `429 Too Many Requests` | Rate limit hit (100 req/min per user) | Wait 60s or increase `app.rate-limiting.requests-per-minute` |
| OutboxEvent stuck at `FAILED` | Kafka unreachable for > max retries (3) | Check Kafka; failed events require manual re-queue |
| Webhook delivery stuck in `FAILED` | Destination URL unreachable | Check `webhook_deliveries` table |
| `DataIntegrityViolationException` on create | Race condition on idempotency key | Expected — `GlobalExceptionHandler` returns 409 Conflict |
| Redis `Connection refused` | Redis container not started | `docker compose up -d redis` |
| High Kafka consumer lag | Slow processing or too few threads | Check `kafka-consumer-groups --describe`; increase concurrency |
| `CRITICAL: Failed to send to DLQ` | Kafka completely unavailable | Manual intervention — check Kafka cluster health |

## Debugging Commands

```bash
# Container status
docker compose ps

# Kafka consumer group lag
docker exec payments-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group payment-processor-group

# PostgreSQL active connections
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

# Reconciliation logs
docker compose logs payment-service | grep "Reconciliation"
```
