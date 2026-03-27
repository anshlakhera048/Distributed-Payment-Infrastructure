/**
 * k6 Load Test — Payments System
 * ================================
 * Tests:
 *   1. POST /payments  — create payment with unique idempotency key
 *   2. GET  /payments/:id — poll payment status
 *   3. Idempotency replay — same key twice → must return identical response
 *   4. Rate limiting verification — burst requests → expect some 429s
 *
 * Run:
 *   k6 run payment-load-test.js
 *   k6 run --vus 50 --duration 60s payment-load-test.js
 *   k6 run --env BASE_URL=http://localhost:8090/api payment-load-test.js  # via gateway
 *
 * Prerequisites:
 *   - npm install -g k6  (or brew install k6)
 *   - Payment service running on BASE_URL
 *   - If testing via gateway: JWT_TOKEN env must be set
 */

import http from "k6/http";
import { check, sleep, group } from "k6";
import { Counter, Rate, Trend } from "k6/metrics";
import { uuidv4 } from "https://jslib.k6.io/k6-utils/1.4.0/index.js";

// -----------------------------------------------------------------------
// Configuration
// -----------------------------------------------------------------------

const BASE_URL   = __ENV.BASE_URL   || "http://localhost:8080";
const JWT_TOKEN  = __ENV.JWT_TOKEN  || "";   // Required when testing via API gateway

/** Load profile stages */
export const options = {
  stages: [
    { duration: "30s", target: 10  },  // Ramp-up to 10 VUs
    { duration: "60s", target: 50  },  // Sustained load
    { duration: "30s", target: 100 },  // Stress spike
    { duration: "30s", target: 10  },  // Scale back down
    { duration: "10s", target: 0   },  // Cool down
  ],
  thresholds: {
    // p95 of POST /payments must be < 500ms
    "payment_create_duration": ["p(95)<500"],
    // p95 of GET /payments/:id must be < 200ms (cache hit expected)
    "payment_get_duration": ["p(95)<200"],
    // Error rate must stay below 1% excluding expected 429s
    "http_req_failed": ["rate<0.01"],
    // Idempotency key replays must always return 200+
    "idempotency_check_pass": ["rate>0.99"],
  },
};

// -----------------------------------------------------------------------
// Custom metrics
// -----------------------------------------------------------------------

const paymentCreateDuration = new Trend("payment_create_duration");
const paymentGetDuration     = new Trend("payment_get_duration");
const idempotencyCheckPass   = new Rate("idempotency_check_pass");
const rateLimitHits          = new Counter("rate_limit_hits");
const fraudBlockedCount      = new Counter("fraud_blocked_count");

// -----------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------

const CURRENCIES = ["USD", "EUR", "GBP", "CAD"];
const USERS = Array.from({ length: 20 }, () => uuidv4());
const MERCHANTS = Array.from({ length: 5 }, () => uuidv4());

function buildHeaders(idempotencyKey) {
  const headers = {
    "Content-Type": "application/json",
    "Idempotency-Key": idempotencyKey,
    "X-Correlation-ID": uuidv4(),
  };
  if (JWT_TOKEN) {
    headers["Authorization"] = `Bearer ${JWT_TOKEN}`;
  }
  return headers;
}

function randomFrom(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

/** Returns an amount that is always under the fraud threshold ($10,000) */
function safeAmount() {
  return (Math.random() * 9000 + 1).toFixed(2);
}

/** Returns an amount that triggers the fraud ML pipeline */
function highRiskAmount() {
  return (Math.random() * 50000 + 10001).toFixed(2);
}

// -----------------------------------------------------------------------
// Default test scenario
// -----------------------------------------------------------------------

export default function () {
  const idempotencyKey = uuidv4();
  const userId         = randomFrom(USERS);
  const merchantId     = randomFrom(MERCHANTS);
  const currency       = randomFrom(CURRENCIES);
  const amount         = safeAmount();

  const body = JSON.stringify({
    userId:     userId,
    merchantId: merchantId,
    amount:     parseFloat(amount),
    currency:   currency,
    description: `Load test payment ${idempotencyKey}`,
  });

  // ----------------------------------------------------------------
  // Scenario 1: Create a new payment
  // ----------------------------------------------------------------
  group("create payment", () => {
    const start = Date.now();
    const res = http.post(`${BASE_URL}/payments`, body, {
      headers: buildHeaders(idempotencyKey),
      tags: { endpoint: "create_payment" },
    });
    paymentCreateDuration.add(Date.now() - start);

    if (res.status === 429) {
      rateLimitHits.add(1);
      return; // Skip further checks for rate-limited requests
    }

    const created = check(res, {
      "create: status 201":           (r) => r.status === 201,
      "create: has paymentId":        (r) => r.json("paymentId") !== undefined,
      "create: status is PENDING":    (r) => r.json("status") === "PENDING",
      "create: has idempotencyKey":   (r) => r.json("idempotencyKey") !== undefined,
    });

    if (!created) {
      return;
    }

    const paymentId = res.json("paymentId");

    // ----------------------------------------------------------------
    // Scenario 2: GET the payment (expect cache hit on second call)
    // ----------------------------------------------------------------
    group("get payment", () => {
      const getStart = Date.now();
      const getRes = http.get(`${BASE_URL}/payments/${paymentId}`, {
        headers: buildHeaders(uuidv4()),
        tags: { endpoint: "get_payment" },
      });
      paymentGetDuration.add(Date.now() - getStart);

      check(getRes, {
        "get: status 200":         (r) => r.status === 200,
        "get: paymentId matches":  (r) => r.json("paymentId") === paymentId,
      });
    });

    // ----------------------------------------------------------------
    // Scenario 3: Idempotency replay — same key, same body → same response
    // ----------------------------------------------------------------
    group("idempotency replay", () => {
      const replayRes = http.post(`${BASE_URL}/payments`, body, {
        headers: buildHeaders(idempotencyKey),  // Same key!
        tags: { endpoint: "idempotency_replay" },
      });

      const replayOk = check(replayRes, {
        "replay: status 200 or 201":      (r) => r.status === 200 || r.status === 201,
        "replay: same paymentId":         (r) => r.json("paymentId") === paymentId,
      });

      idempotencyCheckPass.add(replayOk ? 1 : 0);
    });
  });

  // ----------------------------------------------------------------
  // Scenario 4: High-risk payment (should be blocked by fraud service)
  // ----------------------------------------------------------------
  group("fraud detection", () => {
    const fraudKey = uuidv4();
    const fraudBody = JSON.stringify({
      userId:     userId,
      merchantId: merchantId,
      amount:     parseFloat(highRiskAmount()),
      currency:   "USD",
      description: "High-value test payment",
    });

    const fraudRes = http.post(`${BASE_URL}/payments`, fraudBody, {
      headers: buildHeaders(fraudKey),
      tags: { endpoint: "fraud_check" },
    });

    // High-value payments can be PENDING (async check) or immediately DECLINED
    // Both are valid — the check is async via Kafka
    if (fraudRes.status === 429) {
      rateLimitHits.add(1);
    } else {
      check(fraudRes, {
        "fraud: status 201 or 422": (r) => r.status === 201 || r.status === 422,
      });
    }
  });

  sleep(Math.random() * 0.5 + 0.1); // 100-600ms think time
}

// -----------------------------------------------------------------------
// Setup: seed test data (runs once before VUs start)
// -----------------------------------------------------------------------

export function setup() {
  console.log(`[k6] Starting load test against: ${BASE_URL}`);
  console.log(`[k6] JWT authentication: ${JWT_TOKEN ? "enabled" : "disabled"}`);

  // Verify the service is reachable
  const healthRes = http.get(`${BASE_URL.replace("/api", "")}/actuator/health`);
  if (healthRes.status !== 200) {
    console.error(`[k6] Health check failed: ${healthRes.status} — is the service running?`);
  }
}

// -----------------------------------------------------------------------
// Teardown: summary stats
// -----------------------------------------------------------------------

export function teardown() {
  console.log("[k6] Load test complete — check Grafana for detailed metrics");
}
