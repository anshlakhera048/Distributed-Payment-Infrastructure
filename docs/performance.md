# Performance Engineering Guide

## Table of Contents
1. [Performance Targets](#performance-targets)
2. [Load Testing](#load-testing)
3. [Throughput Analysis](#throughput-analysis)
4. [Latency Breakdown](#latency-breakdown)
5. [Bottleneck Identification](#bottleneck-identification)
6. [Tuning Guide](#tuning-guide)
7. [Backpressure System](#backpressure-system)
8. [Capacity Planning](#capacity-planning)

---

## Performance Targets

| Metric | Target | Measurement |
|--------|--------|-------------|
| Payment creation (p95) | < 500ms | POST /payments end-to-end |
| Payment creation (p99) | < 1000ms | POST /payments end-to-end |
| Payment read (p95) | < 200ms | GET /payments/{id} (cache hit) |
| Payment read (p99) | < 500ms | GET /payments/{id} (cache miss) |
| Throughput (sustained) | 1,000+ TPS | Payment creation rate |
| Throughput (burst) | 2,000 TPS | 30s spike handling |
| Error rate (normal) | < 0.1% | Excluding rate limit 429s |
| Kafka end-to-end latency | < 2s | payment.created → payment.processed |
| Fraud check latency | < 100ms | ML + rule engine combined |

## Load Testing

### Profiles

The system includes four k6 load test profiles:

```bash
# Default: gradual ramp to 100 VUs over 2.5 minutes
k6 run load-tests/payment-load-test.js

# Stress: ramp to 1500 VUs, sustained 1000+ TPS
k6 run --env PROFILE=stress load-tests/payment-load-test.js

# Soak: 200 VUs sustained for 30 minutes (memory leak detection)
k6 run --env PROFILE=soak load-tests/payment-load-test.js

# Spike: instant 2000 VU spike (backpressure validation)
k6 run --env PROFILE=spike load-tests/payment-load-test.js
```

### Via API Gateway (with JWT)
```bash
export JWT_TOKEN="your-jwt-token"
k6 run --env BASE_URL=http://localhost:8090/v1 --env JWT_TOKEN=$JWT_TOKEN load-tests/payment-load-test.js
```

### Custom Metrics
- `payment_create_duration` — POST /payments latency histogram
- `payment_get_duration` — GET /payments/:id latency histogram
- `idempotency_check_pass` — Rate of successful idempotency replays
- `rate_limit_hits` — Counter of 429 responses
- `payment_create_throughput` — Total successful creates
- `backpressure_hits` — Counter of backpressure-induced rejections

## Throughput Analysis

### Critical Path (Payment Creation)
```
HTTP Request
  → CorrelationIdFilter (~0.1ms)
  → PaymentController.createPayment()
    → RateLimiterService.checkRateLimit()     [Redis Lua: ~1ms]
    → PaymentCacheService.getIdempotencyKey() [Redis GET: ~0.5ms]
    → PaymentRepository.findByIdempotencyKey() [PG query: ~2ms]
    → PaymentRepository.save()                [PG INSERT: ~3ms]
    → OutboxPublisherService.createOutboxEvent() [PG INSERT: ~2ms]
    → PaymentCacheService.cachePayment()      [Redis SET: ~0.5ms]
  → HTTP Response
Total: ~10ms typical (p50), ~50ms under load (p95)
```

### Async Path (Fraud Processing)
```
OutboxPublisher (1s poll)
  → Kafka publish payment.created             [~5ms]
PaymentEventConsumer
  → Parse + forward to fraud.request          [~2ms]
Python Fraud Service
  → Rule engine                               [~1ms]
  → ML inference (IsolationForest)            [~5ms]
  → Kafka publish fraud.result                [~5ms]
FraudResultConsumer
  → IdempotentConsumerService.tryMarkProcessed [~2ms]
  → LedgerService.recordPayment               [~5ms]
  → AccountService.debit + credit              [~4ms]
  → PaymentRepository.save(SUCCESS)            [~3ms]
Total: ~1.0-1.5s poll-to-completion
```

## Latency Breakdown

### Database
| Operation | Typical | Under Load | Bottleneck |
|-----------|---------|------------|------------|
| Simple SELECT | 1-2ms | 5-10ms | Connection pool |
| INSERT (single) | 2-3ms | 5-15ms | WAL writes |
| SELECT FOR UPDATE | 3-5ms | 10-30ms | Lock contention |
| Batch INSERT (50) | 5-10ms | 15-30ms | I/O |

### Redis
| Operation | Typical | Under Load |
|-----------|---------|------------|
| GET | 0.3-0.5ms | 1-2ms |
| SET | 0.3-0.5ms | 1-2ms |
| Lua script (rate limit) | 0.5-1ms | 2-3ms |
| INCR + EXPIRE | 0.3-0.5ms | 1ms |

### Kafka
| Operation | Typical | Notes |
|-----------|---------|-------|
| Producer send (acks=all) | 3-5ms | Idempotent, 1 in-flight |
| Consumer poll | 0-500ms | fetch.max.wait.ms |
| End-to-end (produce→consume) | 5-50ms | Depends on batching |

## Bottleneck Identification

### 1. Database Connection Pool
**Symptom:** Increasing p99 latencies, `hikaricp_connections_pending` > 0  
**Config:** `maximum-pool-size: 20`  
**Mitigation:** Increase pool size, optimize queries, add read replicas  
**Monitor:** `hikaricp_connections_active`, `hikaricp_connections_idle`

### 2. Kafka Consumer Lag
**Symptom:** `kafka_consumer_lag_total` increasing  
**Causes:** Slow DB writes, fraud service latency, partition imbalance  
**Mitigation:** Increase consumer concurrency, add partitions, optimize processing  
**Monitor:** `kafka_consumer_lag_total`, `kafka_consumer_lag_max_partition`

### 3. Hot Partitions
**Symptom:** Uneven `kafka_consumer_lag_max_partition` across partitions  
**Cause:** High-volume user sends all events to one partition  
**Mitigation:** Composite partition key (`userId:saltBucket`), `partition-salt-factor: 3`  

### 4. Redis Saturation
**Symptom:** Rate limit fallback to in-memory, `lettuce` timeout errors  
**Config:** `max-active: 20`, `timeout: 2000ms`  
**Mitigation:** Increase pool, add Redis cluster, tune maxmemory policy

## Tuning Guide

### JVM Settings
```yaml
# Recommended for payment-service (production)
JAVA_OPTS: >-
  -Xms512m -Xmx1g
  -XX:+UseG1GC
  -XX:MaxGCPauseMillis=100
  -XX:+HeapDumpOnOutOfMemoryError
```

### HikariCP
```yaml
# Scale connection pool with throughput target
# Rule of thumb: connections = (2 * CPU cores) + spindle_count
hikari:
  maximum-pool-size: 30        # Up from 20 for high throughput
  minimum-idle: 10
  connection-timeout: 20000    # Fail fast under contention
```

### Kafka Consumer
```yaml
# For higher throughput
consumer:
  max-poll-records: 50         # Process more per poll
  fetch.min.bytes: 4096        # Batch more before returning
kafka-listener:
  concurrency: 6               # Match partition count
```

### Kafka Producer
```yaml
producer:
  linger.ms: 10                # Allow more batching
  batch.size: 65536            # Larger batches
```

## Backpressure System

The system implements adaptive backpressure based on Kafka consumer lag:

### Thresholds
| Level | Lag Threshold | Rate Limit Multiplier | Effect |
|-------|--------------|----------------------|--------|
| Normal | < 1,000 | 1.0x | Full throughput |
| Warning | ≥ 1,000 | 1.0x (alert only) | Log warning |
| Critical | ≥ 5,000 | 0.5x | 50% rate reduction |
| Severe | ≥ 10,000 | 0.1x | 90% rate reduction |

### Configuration
```yaml
app:
  backpressure:
    poll-interval-ms: 5000      # Lag polling frequency
    warning-threshold: 1000
    critical-threshold: 5000
    severe-threshold: 10000
```

### Metrics
- `kafka_consumer_lag_total` — Total lag across all partitions
- `kafka_consumer_lag_max_partition` — Hottest partition lag
- `backpressure_activations_total` — Times backpressure kicked in

## Capacity Planning

### Single Instance Limits
| Resource | Limit | Bottleneck |
|----------|-------|------------|
| Max TPS | ~2,000 | DB connection pool |
| Max concurrent users | ~10,000 | Rate limiter cardinality |
| Max Kafka lag | ~50,000 | Consumer processing time |
| Memory (payment-service) | 1GB heap | GC pressure |
| Memory (fraud-service) | 512MB | ML model + feature store |

### Horizontal Scaling
| Component | Scaling Strategy | Coordination |
|-----------|-----------------|--------------|
| Payment Service | Stateless, add pods | DB pool per pod |
| Fraud Service | Stateless, add pods | Kafka consumer group rebalance |
| Kafka | Add partitions + brokers | Rebalance consumer assignments |
| PostgreSQL | Read replicas for queries | Write remains single-primary |
| Redis | Cluster mode | Consistent hashing |

### Scaling Formula
```
Required pods = ceil(target_TPS / single_pod_TPS)
Required DB connections = pods × connections_per_pod
Required Kafka partitions = max(pods × concurrency_per_pod, target_TPS / 500)
```
