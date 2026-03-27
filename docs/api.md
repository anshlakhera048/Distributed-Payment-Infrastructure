# API Reference

## Payment Service (port 8080)

### Create Payment

```
POST /payments
```

**Headers:**

| Header | Required | Description |
|--------|----------|-------------|
| `Content-Type` | Yes | `application/json` |
| `Idempotency-Key` | Yes | Unique key to prevent duplicate payments |
| `X-Trace-ID` | No | Custom trace ID (auto-generated if omitted) |
| `X-Correlation-ID` | No | Custom correlation ID (auto-generated if omitted) |

**Request Body:**

```json
{
  "userId": "550e8400-e29b-41d4-a716-446655440000",
  "merchantId": "660e8400-e29b-41d4-a716-446655440000",
  "amount": 49.99,
  "currency": "USD",
  "description": "Payment description"
}
```

| Field | Type | Validation |
|-------|------|-----------|
| `userId` | UUID | Required |
| `merchantId` | UUID | Required |
| `amount` | Decimal | Required, must be > 0 |
| `currency` | String | Required, 3-letter ISO code |
| `description` | String | Optional |

**Response (201 Created):**

```json
{
  "paymentId": "<uuid>",
  "userId": "550e8400-e29b-41d4-a716-446655440000",
  "merchantId": "660e8400-e29b-41d4-a716-446655440000",
  "amount": 49.99,
  "currency": "USD",
  "status": "PENDING",
  "idempotencyKey": "test-normal-001",
  "description": "Payment description",
  "createdAt": "<timestamp>"
}
```

**Payment Statuses:** `PENDING` → `SUCCESS` | `FAILED` | `FRAUD_REJECTED`

---

### Get Payment

```
GET /payments/{paymentId}
```

**Response (200 OK):** Same shape as create response, with updated `status`, `fraudScore`, and `failureReason` fields.

**Response (404 Not Found):**

```json
{
  "type": "PAYMENT_NOT_FOUND",
  "message": "Payment not found",
  "status": 404
}
```

---

### Register Webhook

```
POST /webhooks
```

**Request Body:**

```json
{
  "merchantId": "660e8400-e29b-41d4-a716-446655440000",
  "url": "https://example.com/webhook",
  "events": "payment.processed,payment.failed"
}
```

**Response:** Returns the webhook endpoint with an HMAC `secret` for signature verification.

---

## Fraud Service (port 8000)

### Score Transaction

```
POST /score
```

**Request Body:**

```json
{
  "payment_id": "p1",
  "user_id": "u1",
  "amount": 100,
  "currency": "USD"
}
```

**Response:**

```json
{
  "payment_id": "p1",
  "fraud": false,
  "score": 0.12,
  "reason": null,
  "stage": "ml"
}
```

**Fraud Triggers:**

| Trigger | Condition | Score | Stage |
|---------|-----------|-------|-------|
| Amount threshold | amount > $10,000 | 0.95 | `rule` |
| Unsupported currency | currency not in allowlist | 0.95 | `rule` |
| Velocity check | >10 transactions in 60s | 0.95 | `rule` |
| ML anomaly | IsolationForest outlier | varies | `ml` |

### Health Check

```
GET /health
```

### Metrics

```
GET /metrics
```

---

## API Gateway (port 8090)

Proxies requests to backend services with authentication and rate limiting.

**Base path:** `/api/*` → routes to Payment Service

**Required:** JWT token (HS256, signed with `GATEWAY_JWT_SECRET`)

**Rate limit:** 100 requests/second, burst 200 (Redis-backed token bucket)

**Headers injected:** `X-Correlation-ID`, `X-User-Id`

**Circuit breaker:** Opens on downstream failures with fallback endpoint

**Retry:** 3 attempts on 502/503/504 for GET requests

---

## Error Responses

All errors follow a consistent format:

```json
{
  "type": "ERROR_TYPE",
  "message": "Human-readable description",
  "status": 400
}
```

| Status | Type | Cause |
|--------|------|-------|
| 400 | `VALIDATION_ERROR` | Missing/invalid fields |
| 400 | `MISSING_HEADER` | Missing `Idempotency-Key` |
| 404 | `PAYMENT_NOT_FOUND` | Payment ID does not exist |
| 409 | `CONFLICT` | Idempotency key race condition |
| 429 | `RATE_LIMIT_EXCEEDED` | Rate limit hit (100 req/min per user) |
| 500 | `INTERNAL_ERROR` | Unexpected server error |

---

## Kafka Topics

| Topic | Producer | Consumer | Purpose |
|-------|----------|----------|---------|
| `payment.created` | Outbox Publisher | PaymentEventConsumer | New payment event |
| `fraud.request` | PaymentEventConsumer | Fraud Service | Fraud scoring request |
| `fraud.result` | Fraud Service | FraudResultConsumer | Fraud scoring result |
| `payment.processed` | Outbox Publisher | PaymentProcessedConsumer | Terminal payment event |
| `payment.failed` | Outbox Publisher | PaymentProcessedConsumer | Failed payment event |
| `*.DLQ` | Any consumer | — | Dead letter queue for unrecoverable errors |

## WebSocket

```
ws://localhost:8080/ws/payments
```

Broadcasts real-time payment status updates. Messages are JSON objects matching the payment response format.
