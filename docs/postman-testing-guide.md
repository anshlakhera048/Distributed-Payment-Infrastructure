# Backend API Testing Guide — Postman

Complete guide to testing every backend endpoint in the Payments System using Postman.

---

## Table of Contents

1. [Service Architecture](#service-architecture)
2. [Setup: Postman Environment](#setup-postman-environment)
3. [Authentication (JWT)](#authentication-jwt)
4. [API Gateway (port 8090)](#api-gateway-port-8090)
5. [Payment Service (port 8080)](#payment-service-port-8080)
6. [Fraud Service (port 8000)](#fraud-service-port-8000)
7. [WebSocket Testing](#websocket-testing)
8. [Webhook System](#webhook-system)
9. [Monitoring Endpoints](#monitoring-endpoints)
10. [Testing Scenarios](#testing-scenarios)
11. [Postman Collection Import](#postman-collection-import)

---

## Service Architecture

```
                       ┌──────────────┐
                       │  Frontend    │
                       │  :5173       │
                       └──────┬───────┘
                              │
                       ┌──────▼───────┐
                       │ API Gateway  │◄─── JWT Validation (HS256)
                       │  :8090       │◄─── Rate Limiting (Redis)
                       │              │◄─── Circuit Breaker
                       └──────┬───────┘
                ┌─────────────┼─────────────┐
                ▼             ▼              ▼
    ┌───────────────┐  ┌──────────┐  ┌──────────────┐
    │Payment Service│  │  Kafka   │  │Fraud Service  │
    │  :8080        │  │  :29092  │  │  :8000        │
    └───────┬───────┘  └──────────┘  └───────────────┘
            │
    ┌───────┼───────┐
    ▼       ▼       ▼
PostgreSQL Redis  Prometheus
  :5433    :6379    :9090
```

| Service | Port | Technology |
|---------|------|------------|
| API Gateway | 8090 | Spring Cloud Gateway (Java 17) |
| Payment Service | 8080 | Spring Boot 3.4.4 (Java 17) |
| Fraud Service | 8000 | FastAPI (Python 3.11) |
| PostgreSQL | 5433 | PostgreSQL 15 |
| Kafka | 29092 | Confluent 7.5 |
| Redis | 6379 | Redis 7 Alpine |
| Prometheus | 9090 | Prometheus v2.47.0 |
| Grafana | 3000 | Grafana 10.1.0 |

---

## Setup: Postman Environment

Create a Postman Environment called **"Payments System — Local"** with these variables:

| Variable | Value | Description |
|----------|-------|-------------|
| `gateway_url` | `http://localhost:8090` | API Gateway (authenticated entry point) |
| `payment_url` | `http://localhost:8080` | Payment Service (direct, no JWT needed) |
| `fraud_url` | `http://localhost:8000` | Fraud Service (direct) |
| `prometheus_url` | `http://localhost:9090` | Prometheus |
| `grafana_url` | `http://localhost:3000` | Grafana |
| `jwt_token` | *(see below)* | Bearer token for gateway auth |
| `admin_api_key` | `changeme-admin-key-for-dev` | Fraud admin API key |

### Generate the JWT

The gateway uses HS256 symmetric JWT validation. For local development, use this pre-signed token (1-year expiry, signed with the default dev secret):

```
eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJkYXNoYm9hcmQtdXNlciIsImlzcyI6InBheW1lbnRzLXN5c3RlbSIsInJvbGVzIjpbImFkbWluIl0sImlhdCI6MTc3NDY0MDg0NSwiZXhwIjoxODA2MTc2ODQ1fQ.0J0jIzXBXAAKXpUI1x7xcku5eTHzxgR8K8pWFLMzwI8
```

Set this as the `jwt_token` environment variable.

**Decoded payload:**
```json
{
  "sub": "dashboard-user",
  "iss": "payments-system",
  "roles": ["admin"],
  "iat": 1774640845,
  "exp": 1806176845
}
```

---

## Authentication (JWT)

All requests through the **API Gateway** (`:8090`) require a JWT Bearer token in the `Authorization` header, except:

| Path | Auth Required |
|------|--------------|
| `GET /actuator/health` | No |
| `GET /actuator/info` | No |
| `GET /actuator/prometheus` | No |
| Everything else (`/api/**`, `/v1/**`) | **Yes** — Bearer token |

**Postman setup:** In your collection's Authorization tab, set:
- **Type:** Bearer Token
- **Token:** `{{jwt_token}}`

This automatically adds `Authorization: Bearer <token>` to every request in the collection.

---

## API Gateway (port 8090)

The gateway is the primary entry point. It adds:
- **JWT validation** (HS256)
- **Rate limiting** (Redis-backed, 50 req/s for payments, 10 req/s for webhooks)
- **Circuit breaker** (opens after 5 failures in 10-call window)
- **Retry** (3 retries on 502/503/504 for GET requests)
- **Correlation ID** injection (`X-Correlation-ID` header)
- **CORS** (allows localhost:5173, localhost:3000)

### Route Mapping

| Gateway Path | Downstream Target | Notes |
|-------------|-------------------|-------|
| `/api/payments/**` | `payment-service:8080/payments/**` | StripPrefix=1 |
| `/v1/payments/**` | `payment-service:8080/payments/**` | Versioned route, StripPrefix=1 |
| `/api/webhooks/**` | `payment-service:8080/webhooks/**` | StripPrefix=1 |
| `/management/**` | `payment-service:8080/management/**` | No strip, internal |

### 1. Gateway Health Check

```
GET {{gateway_url}}/actuator/health
```

No authentication needed.

**Expected Response** (`200 OK`):
```json
{
  "status": "UP",
  "components": {
    "redis": { "status": "UP", "details": { "version": "7.4.8" } },
    "ping": { "status": "UP" },
    "diskSpace": { "status": "UP" }
  }
}
```

### 2. Gateway Info

```
GET {{gateway_url}}/actuator/info
```

### 3. Gateway Route Definitions

```
GET {{gateway_url}}/actuator/gateway/routes
```

**Requires:** Bearer token.

---

## Payment Service (port 8080)

### Request & Response Schemas

**CreatePaymentRequest:**
```json
{
  "userId": "UUID (required)",
  "merchantId": "UUID (required)",
  "amount": "decimal > 0.01 (required)",
  "currency": "3 uppercase letters, ISO 4217 (required) — e.g. USD, EUR, GBP, INR, JPY, CAD, AUD",
  "description": "string, max 512 chars (optional)"
}
```

**PaymentResponse:**
```json
{
  "paymentId": "UUID",
  "userId": "UUID",
  "merchantId": "UUID",
  "amount": 42.50,
  "currency": "USD",
  "status": "PENDING | SUCCESS | FAILED | FRAUD_REJECTED",
  "idempotencyKey": "string",
  "description": "string | null",
  "createdAt": "2026-03-29T18:46:18"
}
```

**ErrorResponse:**
```json
{
  "type": "VALIDATION_FAILED | PAYMENT_NOT_FOUND | RATE_LIMIT_EXCEEDED | ...",
  "message": "Human-readable description",
  "status": 400,
  "correlationId": "UUID",
  "path": "/payments",
  "timestamp": "2026-03-29T18:46:18"
}
```

**Payment Statuses:**
| Status | Meaning |
|--------|---------|
| `PENDING` | Created, waiting for fraud check |
| `SUCCESS` | Fraud check passed, ledger balanced |
| `FAILED` | Insufficient balance or processing error |
| `FRAUD_REJECTED` | Fraud service flagged the transaction |
| `RECONCILED` | Confirmed during reconciliation cycle |

---

### 1. Create Payment (via Gateway)

```
POST {{gateway_url}}/api/payments
```

**Headers:**
| Header | Value | Required |
|--------|-------|----------|
| `Authorization` | `Bearer {{jwt_token}}` | Yes |
| `Content-Type` | `application/json` | Yes |
| `Idempotency-Key` | Any unique string (UUID recommended) | **Yes** |

**Body (raw JSON):**
```json
{
  "userId": "11111111-1111-1111-1111-111111111111",
  "merchantId": "22222222-2222-2222-2222-222222222222",
  "amount": 250.00,
  "currency": "USD",
  "description": "Order #12345"
}
```

**Expected Response** (`201 Created`):
```json
{
  "paymentId": "fd546190-8519-46a9-b2b6-d9fd9aec2ed7",
  "userId": "11111111-1111-1111-1111-111111111111",
  "merchantId": "22222222-2222-2222-2222-222222222222",
  "amount": 250.00,
  "currency": "USD",
  "status": "PENDING",
  "idempotencyKey": "your-key-here",
  "description": "Order #12345",
  "createdAt": "2026-03-29T18:46:18"
}
```

> **Note:** The payment starts as `PENDING`. Within 1-3 seconds, it transitions to `SUCCESS`, `FAILED`, or `FRAUD_REJECTED` after the async Kafka fraud pipeline processes it.

**Postman Tip — Auto-generate Idempotency-Key:**
In the Pre-request Script tab, add:
```javascript
pm.request.headers.add({
    key: 'Idempotency-Key',
    value: pm.variables.replaceIn('{{$guid}}')
});
```

### 2. Create Payment (via versioned route)

```
POST {{gateway_url}}/v1/payments
```

Same headers and body as above. This is the versioned API route (`/v1/`), functionally identical to `/api/`.

### 3. Create Payment (direct — no JWT)

```
POST {{payment_url}}/payments
```

Same headers and body, but **no `Authorization` header needed** since you're hitting the payment service directly (bypasses gateway). Useful for debugging.

### 4. Get Payment by ID

```
GET {{gateway_url}}/api/payments/{{paymentId}}
```

**Headers:**
| Header | Value |
|--------|-------|
| `Authorization` | `Bearer {{jwt_token}}` |

**Expected Response** (`200 OK`):
```json
{
  "paymentId": "fd546190-8519-46a9-b2b6-d9fd9aec2ed7",
  "userId": "11111111-1111-1111-1111-111111111111",
  "merchantId": "22222222-2222-2222-2222-222222222222",
  "amount": 250.00,
  "currency": "USD",
  "status": "SUCCESS",
  "idempotencyKey": "your-key-here",
  "description": "Order #12345",
  "createdAt": "2026-03-29T18:46:18"
}
```

**Postman Tip — Save `paymentId` from Create response:**
In the Create Payment's Tests tab:
```javascript
const res = pm.response.json();
pm.environment.set("paymentId", res.paymentId);
```

### 5. List Payments (Paginated)

```
GET {{gateway_url}}/api/payments?page=0&size=10
```

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `page` | int | 0 | Page number (0-indexed) |
| `size` | int | 20 | Page size (max 100) |
| `userId` | UUID | — | Filter by user ID |
| `status` | enum | — | Filter by status: `PENDING`, `SUCCESS`, `FAILED`, `FRAUD_REJECTED` |

**Examples:**
```
GET {{gateway_url}}/api/payments?page=0&size=5
GET {{gateway_url}}/api/payments?userId=11111111-1111-1111-1111-111111111111
GET {{gateway_url}}/api/payments?status=SUCCESS&page=0&size=10
GET {{gateway_url}}/api/payments?userId=11111111-1111-1111-1111-111111111111&status=FAILED
```

**Expected Response** (`200 OK`):
```json
{
  "content": [
    {
      "paymentId": "...",
      "userId": "...",
      "amount": 250.00,
      "currency": "USD",
      "status": "SUCCESS",
      "createdAt": "2026-03-29T18:46:18"
    }
  ],
  "pageable": {
    "pageNumber": 0,
    "pageSize": 10,
    "sort": { "sorted": true, "unsorted": false }
  },
  "totalPages": 6,
  "totalElements": 55,
  "first": true,
  "last": false,
  "numberOfElements": 10,
  "empty": false
}
```

### 6. Payment Service Health (direct)

```
GET {{payment_url}}/actuator/health
```

No authentication required. Returns health of PostgreSQL, Redis, and disk.

### 7. Payment Service Prometheus Metrics

```
GET {{payment_url}}/actuator/prometheus
```

Returns Prometheus-format metrics including:
- `payments_created_total` — payment creation counter by currency
- `payments_processed_total` — processing outcome counter by status
- `fraud_detected_total` — fraud detection counter by reason
- `kafka_consumer_lag_total` — Kafka consumer group lag
- `rate_limit_exceeded_total` — rate limit rejection counter
- Standard JVM, Hikari pool, HTTP request metrics

---

## Fraud Service (port 8000)

### 1. Service Info (Root)

```
GET {{fraud_url}}/
```

**Expected Response** (`200 OK`):
```json
{
  "service": "fraud-detection-service",
  "version": "1.0.0",
  "status": "running",
  "endpoints": ["/score", "/health", "/metrics", "/admin/model"]
}
```

### 2. Health Check

```
GET {{fraud_url}}/health
```

**Expected Response** (`200 OK`):
```json
{
  "status": "ok",
  "model": "v1.0",
  "feature_count": 3,
  "feature_store": "redis"
}
```

### 3. Score a Transaction (Manual Fraud Check)

```
POST {{fraud_url}}/score
```

**Headers:**
| Header | Value |
|--------|-------|
| `Content-Type` | `application/json` |

**Body:**
```json
{
  "payment_id": "test-001",
  "user_id": "user-123",
  "amount": 500.00,
  "currency": "USD",
  "correlation_id": "corr-001"
}
```

**Expected Response** (`200 OK`):
```json
{
  "payment_id": "test-001",
  "fraud": false,
  "reason": "ok",
  "score": 0.12,
  "fallback": false,
  "stage": "ml"
}
```

**Fraud Detection Triggers:**
| Trigger | Rule | Result |
|---------|------|--------|
| Amount > $10,000 | Rule-based | `fraud: true`, `reason: "amount_exceeds_threshold"` |
| Unsupported currency | Rule-based | `fraud: true`, `reason: "unsupported_currency"` |
| >10 requests/60s per user | Velocity check | `fraud: true`, `reason: "velocity_exceeded"` |
| ML score ≥ 0.75 | ML model | `fraud: true`, `reason: "ml_high_risk_score"` |

**Test: Trigger amount fraud:**
```json
{
  "payment_id": "fraud-test-001",
  "user_id": "user-456",
  "amount": 15000.00,
  "currency": "USD"
}
```

**Test: Trigger unsupported currency:**
```json
{
  "payment_id": "fraud-test-002",
  "user_id": "user-456",
  "amount": 100.00,
  "currency": "BTC"
}
```

Allowed currencies: `USD`, `EUR`, `GBP`, `INR`, `JPY`, `CAD`, `AUD`.

### 4. ML Model Info (Admin — Protected)

```
GET {{fraud_url}}/admin/model
```

**Headers:**
| Header | Value |
|--------|-------|
| `X-Admin-Key` | `{{admin_api_key}}` |

**Expected Response** (`200 OK`):
```json
{
  "version": "v1.0",
  "feature_count": 3,
  "trained_at": 1774809749.56,
  "training_samples": 1000,
  "contamination": 0.1
}
```

**Without API key → `403 Forbidden`:**
```json
{ "detail": "Invalid or missing admin API key" }
```

### 5. Train v2 Model (Admin — Protected)

```
POST {{fraud_url}}/admin/model/train-v2
```

**Headers:**
| Header | Value |
|--------|-------|
| `X-Admin-Key` | `{{admin_api_key}}` |

Trains an enhanced 11-feature IsolationForest model. After training, the model version changes to `v2.0` and `feature_count` becomes `11`.

**Expected Response** (`200 OK`):
```json
{
  "status": "ok",
  "model": {
    "version": "v2.0",
    "feature_count": 11,
    "trained_at": 1774810512.34,
    "training_samples": 2000,
    "contamination": 0.08
  }
}
```

### 6. Prometheus Metrics

```
GET {{fraud_url}}/metrics
```

Returns Prometheus-format metrics:
- `fraud_requests_total{result, stage}` — request counter by fraud result and detection stage
- `fraud_request_duration_seconds` — latency histogram
- `fraud_score_distribution` — ML score distribution histogram

---

## WebSocket Testing

The Payment Service exposes a real-time WebSocket endpoint that broadcasts payment events (created, processed, failed, fraud alerts).

### Connection

```
ws://localhost:8080/ws/payments
```

**In Postman:**
1. Create a new **WebSocket Request** (not HTTP)
2. Enter URL: `ws://localhost:8080/ws/payments`
3. Click **Connect**

### Messages

**On connect** you'll receive:
```json
{ "type": "CONNECTED", "message": "Connected to payment updates stream" }
```

**Ping/Pong:** Send `ping` as text → receive:
```json
{ "type": "PONG" }
```

**Payment events** are broadcast automatically when payments are created/processed:
```json
{
  "version": "v1",
  "eventId": "uuid",
  "traceId": "uuid",
  "correlationId": "uuid",
  "eventType": "payment.created",
  "paymentId": "uuid",
  "userId": "uuid",
  "merchantId": "uuid",
  "amount": 250.00,
  "currency": "USD",
  "status": "PENDING",
  "idempotencyKey": "string",
  "description": "Order #12345",
  "createdAt": "2026-03-29T18:46:18",
  "eventTimestamp": "2026-03-29T18:46:18"
}
```

**Event Types:**  
| eventType | When |
|-----------|------|
| `payment.created` | New payment created (status: PENDING) |
| `payment.processed` | Fraud check passed, payment succeeded |
| `payment.failed` | Insufficient balance or processing error |
| `fraud.alerts` | Fraud detected and payment rejected |

### Testing flow:
1. Connect to WebSocket in one Postman tab
2. Create a payment via `POST /api/payments` in another tab
3. Watch real-time events arrive on the WebSocket

---

## Webhook System

Register merchant webhook endpoints to receive payment event callbacks.

### 1. Register Webhook

```
POST {{gateway_url}}/api/webhooks
```

**Headers:**
| Header | Value |
|--------|-------|
| `Authorization` | `Bearer {{jwt_token}}` |
| `Content-Type` | `application/json` |

**Body:**
```json
{
  "merchantId": "22222222-2222-2222-2222-222222222222",
  "url": "https://your-domain.com/webhook/payments",
  "events": "payment.processed,payment.failed,fraud.alerts"
}
```

> **Security:** The `url` field must be an HTTPS URL with a public domain. Private IPs, localhost, and HTTP are rejected (SSRF protection).

**Use `"ALL"` to subscribe to all event types:**
```json
{
  "merchantId": "22222222-2222-2222-2222-222222222222",
  "url": "https://webhook.site/your-unique-id",
  "events": "ALL"
}
```

> **Tip:** Use [webhook.site](https://webhook.site) to get a free HTTPS URL for testing.

**Expected Response** (`201 Created`):
```json
{
  "id": "uuid",
  "url": "https://your-domain.com/webhook/payments",
  "events": "payment.processed,payment.failed,fraud.alerts",
  "secret": "a1b2c3d4e5f6... (signing secret — save this!)",
  "active": true
}
```

> **Keep the `secret`** — it's used to sign webhook payloads (HMAC). You can use it to verify webhook authenticity on your server.

### 2. Deactivate Webhook

```
DELETE {{gateway_url}}/api/webhooks/{{webhookId}}
```

**Expected Response:** `204 No Content`

---

## Monitoring Endpoints

### Prometheus Targets

```
GET {{prometheus_url}}/api/v1/targets
```

Verify all three services are being scraped:
- `payment-service` → `:8080/actuator/prometheus`
- `api-gateway` → `:8090/actuator/prometheus`
- `fraud-service` → `:8000/metrics`

### Prometheus Query Examples

```
GET {{prometheus_url}}/api/v1/query?query=payments_created_total
GET {{prometheus_url}}/api/v1/query?query=rate(payments_processed_total[5m])
GET {{prometheus_url}}/api/v1/query?query=kafka_consumer_lag_total
GET {{prometheus_url}}/api/v1/query?query=fraud_requests_total
```

### Grafana

```
GET {{grafana_url}}/api/search
```

**Auth:** Basic Auth → `admin` / `admin`

A pre-provisioned **"Payments System Overview"** dashboard is available at:
```
http://localhost:3000/d/payments-overview
```

Panels include: payment creation rate, processing outcomes, fraud detection, Kafka lag, API gateway request rate, p95/p99 latency, circuit breaker state, JVM heap usage.

---

## Testing Scenarios

### Scenario 1: Happy Path — Payment Success

1. **Create Payment:**
   ```
   POST {{gateway_url}}/api/payments
   Headers: Authorization, Content-Type, Idempotency-Key: {{$guid}}
   Body: { userId, merchantId, amount: 100, currency: "USD" }
   ```
2. **Wait 2-3 seconds** (Kafka fraud pipeline)
3. **Check Payment Status:**
   ```
   GET {{gateway_url}}/api/payments/{{paymentId}}
   ```
4. **Verify:** `status` should be `"SUCCESS"`

### Scenario 2: Fraud Rejection — High Amount

1. **Create Payment with amount > $10,000:**
   ```json
   {
     "userId": "11111111-1111-1111-1111-111111111111",
     "merchantId": "22222222-2222-2222-2222-222222222222",
     "amount": 15000.00,
     "currency": "USD"
   }
   ```
2. **Wait 2-3 seconds**
3. **Check Status:** should be `"FRAUD_REJECTED"`

### Scenario 3: Fraud Rejection — Unsupported Currency

1. **Score directly:**
   ```
   POST {{fraud_url}}/score
   Body: { "payment_id": "test", "user_id": "u1", "amount": 100, "currency": "BTC" }
   ```
2. **Verify:** `fraud: true`, `reason: "unsupported_currency"`

### Scenario 4: Idempotency — Duplicate Prevention

1. **Set a fixed Idempotency-Key** (e.g., `test-idemp-001`)
2. **Send `POST /api/payments`** — get `201 Created` with a `paymentId`
3. **Send the EXACT same request** with the same `Idempotency-Key`
4. **Verify:** Returns the same `paymentId` (no duplicate created)

### Scenario 5: Validation Errors

**Missing currency:**
```json
{ "userId": "11111111-1111-1111-1111-111111111111", "merchantId": "...", "amount": 100 }
```
→ `400 Bad Request`, type: `VALIDATION_FAILED`

**Invalid currency format (lowercase):**
```json
{ "userId": "...", "merchantId": "...", "amount": 100, "currency": "usd" }
```
→ `400`, message: `"currency: Currency must be 3 uppercase letters (ISO 4217)"`

**Amount zero or negative:**
```json
{ "userId": "...", "merchantId": "...", "amount": 0, "currency": "USD" }
```
→ `400`, message: `"amount: Amount must be greater than zero"`

**Missing Idempotency-Key header:**
→ `400`, message: `"Required header missing: Idempotency-Key"`

### Scenario 6: Rate Limiting

Send > 100 requests in quick succession from the same user:
```javascript
// Postman Collection Runner or Newman
// Each request: POST /api/payments with unique Idempotency-Key
```
→ Eventually returns `429 Too Many Requests`

### Scenario 7: Payment Not Found

```
GET {{gateway_url}}/api/payments/00000000-0000-0000-0000-000000000000
```
→ `404 Not Found`, type: `PAYMENT_NOT_FOUND`

### Scenario 8: Gateway Without JWT

```
POST {{gateway_url}}/api/payments
```
(No `Authorization` header)
→ `401 Unauthorized`

### Scenario 9: Velocity Fraud (>10 txn/min per user)

Send 11+ fraud score requests in under 60 seconds for the same `user_id`:
```
POST {{fraud_url}}/score
Body: { "payment_id": "vel-1", "user_id": "same-user", "amount": 50, "currency": "USD" }
```
Repeat 11 times → the 11th returns:
```json
{ "fraud": true, "reason": "velocity_exceeded:11_in_60s", "stage": "rule" }
```

### Scenario 10: Circuit Breaker Fallback

1. Stop the payment service: `docker stop payments-payment-service`
2. Send request through gateway:
   ```
   GET {{gateway_url}}/api/payments/any-id
   ```
3. After a few failures, circuit breaker opens → returns `503 Service Unavailable`:
   ```json
   { "message": "Payment service is temporarily unavailable. Please retry shortly." }
   ```
4. Restart: `docker start payments-payment-service`

### Scenario 11: Pagination

```
GET {{gateway_url}}/api/payments?page=0&size=3
```
Verify `totalPages`, `totalElements`, `first`, `last`, `numberOfElements` in response.

Navigate pages:
```
GET {{gateway_url}}/api/payments?page=1&size=3   (second page)
GET {{gateway_url}}/api/payments?page=0&size=100  (max page size)
```

### Scenario 12: Filter by Status

```
GET {{gateway_url}}/api/payments?status=FRAUD_REJECTED&size=5
```
All results should have `"status": "FRAUD_REJECTED"`.

---

## Postman Collection Import

Below is a ready-to-import Postman collection in JSON format. Save it as `payments-system.postman_collection.json` and import via **File → Import** in Postman.

```json
{
  "info": {
    "name": "Payments System",
    "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"
  },
  "variable": [
    { "key": "gateway_url", "value": "http://localhost:8090" },
    { "key": "payment_url", "value": "http://localhost:8080" },
    { "key": "fraud_url", "value": "http://localhost:8000" },
    { "key": "prometheus_url", "value": "http://localhost:9090" },
    { "key": "grafana_url", "value": "http://localhost:3000" },
    { "key": "jwt_token", "value": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJkYXNoYm9hcmQtdXNlciIsImlzcyI6InBheW1lbnRzLXN5c3RlbSIsInJvbGVzIjpbImFkbWluIl0sImlhdCI6MTc3NDY0MDg0NSwiZXhwIjoxODA2MTc2ODQ1fQ.0J0jIzXBXAAKXpUI1x7xcku5eTHzxgR8K8pWFLMzwI8" },
    { "key": "admin_api_key", "value": "changeme-admin-key-for-dev" },
    { "key": "paymentId", "value": "" },
    { "key": "webhookId", "value": "" }
  ],
  "auth": {
    "type": "bearer",
    "bearer": [{ "key": "token", "value": "{{jwt_token}}" }]
  },
  "item": [
    {
      "name": "Gateway",
      "item": [
        {
          "name": "Health Check",
          "request": { "method": "GET", "url": "{{gateway_url}}/actuator/health", "auth": { "type": "noauth" } }
        },
        {
          "name": "Gateway Routes",
          "request": { "method": "GET", "url": "{{gateway_url}}/actuator/gateway/routes" }
        }
      ]
    },
    {
      "name": "Payments",
      "item": [
        {
          "name": "Create Payment",
          "event": [
            {
              "listen": "prerequest",
              "script": { "exec": ["pm.request.headers.add({ key: 'Idempotency-Key', value: pm.variables.replaceIn('{{$guid}}') });"] }
            },
            {
              "listen": "test",
              "script": { "exec": [
                "pm.test('Status is 201', () => pm.response.to.have.status(201));",
                "const res = pm.response.json();",
                "pm.environment.set('paymentId', res.paymentId);",
                "pm.test('Status is PENDING', () => pm.expect(res.status).to.eql('PENDING'));"
              ] }
            }
          ],
          "request": {
            "method": "POST",
            "url": "{{gateway_url}}/api/payments",
            "header": [{ "key": "Content-Type", "value": "application/json" }],
            "body": { "mode": "raw", "raw": "{\n  \"userId\": \"11111111-1111-1111-1111-111111111111\",\n  \"merchantId\": \"22222222-2222-2222-2222-222222222222\",\n  \"amount\": 250.00,\n  \"currency\": \"USD\",\n  \"description\": \"Postman test payment\"\n}" }
          }
        },
        {
          "name": "Get Payment by ID",
          "event": [
            {
              "listen": "test",
              "script": { "exec": [
                "pm.test('Status is 200', () => pm.response.to.have.status(200));",
                "const res = pm.response.json();",
                "pm.test('Has paymentId', () => pm.expect(res.paymentId).to.not.be.undefined);"
              ] }
            }
          ],
          "request": { "method": "GET", "url": "{{gateway_url}}/api/payments/{{paymentId}}" }
        },
        {
          "name": "List Payments",
          "request": { "method": "GET", "url": { "raw": "{{gateway_url}}/api/payments?page=0&size=10", "host": ["{{gateway_url}}"], "path": ["api", "payments"], "query": [{ "key": "page", "value": "0" }, { "key": "size", "value": "10" }] } }
        },
        {
          "name": "List Payments (filter by status)",
          "request": { "method": "GET", "url": { "raw": "{{gateway_url}}/api/payments?status=SUCCESS&size=5", "host": ["{{gateway_url}}"], "path": ["api", "payments"], "query": [{ "key": "status", "value": "SUCCESS" }, { "key": "size", "value": "5" }] } }
        },
        {
          "name": "List Payments (filter by userId)",
          "request": { "method": "GET", "url": { "raw": "{{gateway_url}}/api/payments?userId=11111111-1111-1111-1111-111111111111&size=5", "host": ["{{gateway_url}}"], "path": ["api", "payments"], "query": [{ "key": "userId", "value": "11111111-1111-1111-1111-111111111111" }, { "key": "size", "value": "5" }] } }
        },
        {
          "name": "Payment Not Found (404)",
          "event": [
            { "listen": "test", "script": { "exec": ["pm.test('Status is 404', () => pm.response.to.have.status(404));"] } }
          ],
          "request": { "method": "GET", "url": "{{gateway_url}}/api/payments/00000000-0000-0000-0000-000000000000" }
        },
        {
          "name": "Create Fraud Payment (>$10k)",
          "event": [
            {
              "listen": "prerequest",
              "script": { "exec": ["pm.request.headers.add({ key: 'Idempotency-Key', value: pm.variables.replaceIn('{{$guid}}') });"] }
            },
            {
              "listen": "test",
              "script": { "exec": ["pm.test('Status is 201', () => pm.response.to.have.status(201));", "pm.environment.set('fraudPaymentId', pm.response.json().paymentId);"] }
            }
          ],
          "request": {
            "method": "POST",
            "url": "{{gateway_url}}/api/payments",
            "header": [{ "key": "Content-Type", "value": "application/json" }],
            "body": { "mode": "raw", "raw": "{\n  \"userId\": \"11111111-1111-1111-1111-111111111111\",\n  \"merchantId\": \"22222222-2222-2222-2222-222222222222\",\n  \"amount\": 15000.00,\n  \"currency\": \"USD\",\n  \"description\": \"High amount — should trigger fraud\"\n}" }
          }
        },
        {
          "name": "Create Payment (invalid currency)",
          "event": [
            {
              "listen": "prerequest",
              "script": { "exec": ["pm.request.headers.add({ key: 'Idempotency-Key', value: pm.variables.replaceIn('{{$guid}}') });"] }
            },
            {
              "listen": "test",
              "script": { "exec": ["pm.test('Status is 400', () => pm.response.to.have.status(400));"] }
            }
          ],
          "request": {
            "method": "POST",
            "url": "{{gateway_url}}/api/payments",
            "header": [{ "key": "Content-Type", "value": "application/json" }],
            "body": { "mode": "raw", "raw": "{\n  \"userId\": \"11111111-1111-1111-1111-111111111111\",\n  \"merchantId\": \"22222222-2222-2222-2222-222222222222\",\n  \"amount\": 100.00,\n  \"currency\": \"xyz\"\n}" }
          }
        }
      ]
    },
    {
      "name": "Fraud Service",
      "item": [
        {
          "name": "Service Info (Root)",
          "request": { "method": "GET", "url": "{{fraud_url}}/", "auth": { "type": "noauth" } }
        },
        {
          "name": "Health Check",
          "request": { "method": "GET", "url": "{{fraud_url}}/health", "auth": { "type": "noauth" } }
        },
        {
          "name": "Score Transaction (clean)",
          "request": {
            "method": "POST",
            "url": "{{fraud_url}}/score",
            "auth": { "type": "noauth" },
            "header": [{ "key": "Content-Type", "value": "application/json" }],
            "body": { "mode": "raw", "raw": "{\n  \"payment_id\": \"test-clean-001\",\n  \"user_id\": \"user-clean\",\n  \"amount\": 100.00,\n  \"currency\": \"USD\"\n}" }
          }
        },
        {
          "name": "Score Transaction (high amount → fraud)",
          "request": {
            "method": "POST",
            "url": "{{fraud_url}}/score",
            "auth": { "type": "noauth" },
            "header": [{ "key": "Content-Type", "value": "application/json" }],
            "body": { "mode": "raw", "raw": "{\n  \"payment_id\": \"test-fraud-001\",\n  \"user_id\": \"user-fraud\",\n  \"amount\": 15000.00,\n  \"currency\": \"USD\"\n}" }
          }
        },
        {
          "name": "Score Transaction (bad currency → fraud)",
          "request": {
            "method": "POST",
            "url": "{{fraud_url}}/score",
            "auth": { "type": "noauth" },
            "header": [{ "key": "Content-Type", "value": "application/json" }],
            "body": { "mode": "raw", "raw": "{\n  \"payment_id\": \"test-fraud-002\",\n  \"user_id\": \"user-fraud\",\n  \"amount\": 100.00,\n  \"currency\": \"BTC\"\n}" }
          }
        },
        {
          "name": "Admin — Model Info (with API key)",
          "request": {
            "method": "GET",
            "url": "{{fraud_url}}/admin/model",
            "auth": { "type": "noauth" },
            "header": [{ "key": "X-Admin-Key", "value": "{{admin_api_key}}" }]
          }
        },
        {
          "name": "Admin — Model Info (no key → 403)",
          "event": [
            { "listen": "test", "script": { "exec": ["pm.test('Status is 403', () => pm.response.to.have.status(403));"] } }
          ],
          "request": {
            "method": "GET",
            "url": "{{fraud_url}}/admin/model",
            "auth": { "type": "noauth" }
          }
        },
        {
          "name": "Admin — Train v2 Model",
          "request": {
            "method": "POST",
            "url": "{{fraud_url}}/admin/model/train-v2",
            "auth": { "type": "noauth" },
            "header": [{ "key": "X-Admin-Key", "value": "{{admin_api_key}}" }]
          }
        },
        {
          "name": "Prometheus Metrics",
          "request": { "method": "GET", "url": "{{fraud_url}}/metrics", "auth": { "type": "noauth" } }
        }
      ]
    },
    {
      "name": "Webhooks",
      "item": [
        {
          "name": "Register Webhook",
          "event": [
            {
              "listen": "test",
              "script": { "exec": ["pm.test('Status is 201', () => pm.response.to.have.status(201));", "pm.environment.set('webhookId', pm.response.json().id);"] }
            }
          ],
          "request": {
            "method": "POST",
            "url": "{{gateway_url}}/api/webhooks",
            "header": [{ "key": "Content-Type", "value": "application/json" }],
            "body": { "mode": "raw", "raw": "{\n  \"merchantId\": \"22222222-2222-2222-2222-222222222222\",\n  \"url\": \"https://webhook.site/your-unique-id\",\n  \"events\": \"ALL\"\n}" }
          }
        },
        {
          "name": "Deactivate Webhook",
          "request": { "method": "DELETE", "url": "{{gateway_url}}/api/webhooks/{{webhookId}}" }
        }
      ]
    },
    {
      "name": "Monitoring",
      "item": [
        {
          "name": "Prometheus Targets",
          "request": { "method": "GET", "url": "{{prometheus_url}}/api/v1/targets", "auth": { "type": "noauth" } }
        },
        {
          "name": "Prometheus — Payment Count",
          "request": { "method": "GET", "url": { "raw": "{{prometheus_url}}/api/v1/query?query=payments_created_total", "host": ["{{prometheus_url}}"], "path": ["api", "v1", "query"], "query": [{ "key": "query", "value": "payments_created_total" }] }, "auth": { "type": "noauth" } }
        },
        {
          "name": "Prometheus — Kafka Lag",
          "request": { "method": "GET", "url": { "raw": "{{prometheus_url}}/api/v1/query?query=kafka_consumer_lag_total", "host": ["{{prometheus_url}}"], "path": ["api", "v1", "query"], "query": [{ "key": "query", "value": "kafka_consumer_lag_total" }] }, "auth": { "type": "noauth" } }
        },
        {
          "name": "Grafana — Search Dashboards",
          "request": { "method": "GET", "url": "{{grafana_url}}/api/search", "auth": { "type": "basic", "basic": [{ "key": "username", "value": "admin" }, { "key": "password", "value": "admin" }] } }
        }
      ]
    },
    {
      "name": "Direct (bypass gateway)",
      "item": [
        {
          "name": "Payment Service Health",
          "request": { "method": "GET", "url": "{{payment_url}}/actuator/health", "auth": { "type": "noauth" } }
        },
        {
          "name": "Payment Service Metrics",
          "request": { "method": "GET", "url": "{{payment_url}}/actuator/prometheus", "auth": { "type": "noauth" } }
        },
        {
          "name": "Create Payment (direct, no JWT)",
          "event": [
            {
              "listen": "prerequest",
              "script": { "exec": ["pm.request.headers.add({ key: 'Idempotency-Key', value: pm.variables.replaceIn('{{$guid}}') });"] }
            }
          ],
          "request": {
            "method": "POST",
            "url": "{{payment_url}}/payments",
            "auth": { "type": "noauth" },
            "header": [{ "key": "Content-Type", "value": "application/json" }],
            "body": { "mode": "raw", "raw": "{\n  \"userId\": \"11111111-1111-1111-1111-111111111111\",\n  \"merchantId\": \"22222222-2222-2222-2222-222222222222\",\n  \"amount\": 75.00,\n  \"currency\": \"EUR\",\n  \"description\": \"Direct payment (no gateway)\"\n}" }
          }
        }
      ]
    }
  ]
}
```

**To import:**
1. Copy the JSON above into a file named `payments-system.postman_collection.json`
2. In Postman: **File → Import → Upload Files → select the JSON**
3. Set up the environment variables as described in the [Setup section](#setup-postman-environment)
4. Start testing!

---

## Quick Reference — All Endpoints

| Method | Path (via Gateway :8090) | Auth | Description |
|--------|--------------------------|------|-------------|
| `GET` | `/actuator/health` | No | Gateway health |
| `GET` | `/actuator/info` | No | Gateway info |
| `GET` | `/actuator/prometheus` | No | Gateway metrics |
| `POST` | `/api/payments` | JWT | Create payment |
| `GET` | `/api/payments` | JWT | List payments (paginated) |
| `GET` | `/api/payments/{id}` | JWT | Get payment by ID |
| `POST` | `/v1/payments` | JWT | Create payment (versioned) |
| `GET` | `/v1/payments/{id}` | JWT | Get payment (versioned) |
| `POST` | `/api/webhooks` | JWT | Register webhook |
| `DELETE` | `/api/webhooks/{id}` | JWT | Deactivate webhook |

| Method | Path (Payment Service :8080) | Auth | Description |
|--------|------------------------------|------|-------------|
| `POST` | `/payments` | No | Create payment |
| `GET` | `/payments` | No | List payments |
| `GET` | `/payments/{id}` | No | Get payment |
| `POST` | `/webhooks` | No | Register webhook |
| `DELETE` | `/webhooks/{id}` | No | Deactivate webhook |
| `GET` | `/actuator/health` | No | Health check |
| `GET` | `/actuator/prometheus` | No | Prometheus metrics |
| `WS` | `/ws/payments` | No | WebSocket stream |

| Method | Path (Fraud Service :8000) | Auth | Description |
|--------|----------------------------|------|-------------|
| `GET` | `/` | No | Service info |
| `GET` | `/health` | No | Health check |
| `POST` | `/score` | No | Score transaction |
| `GET` | `/metrics` | No | Prometheus metrics |
| `GET` | `/admin/model` | API Key | ML model metadata |
| `POST` | `/admin/model/train-v2` | API Key | Train v2 model |
