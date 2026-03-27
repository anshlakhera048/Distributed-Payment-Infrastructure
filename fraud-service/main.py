"""
Fraud Detection Microservice
============================
Two-stage fraud pipeline:

1. Rule-based checks (fast, deterministic):
   - Velocity: >10 transactions in 60 seconds for the same user
   - Threshold: amount > $10,000
   - Currency allowlist

2. ML-based scoring (sklearn IsolationForest):
   - Features: amount, is_high_value, currency_risk_score
   - Trained on synthetic normal/fraud patterns at startup
   - Score 0.0 = clean, 1.0 = highly suspicious

Transport modes:
   - HTTP API (POST /score): synchronous, used for direct integration / testing
   - Kafka pipeline: async, production-grade decoupled pipeline
       fraud.request  →  [this service]  →  fraud.result

Circuit breaker pattern: if both rule-engine and ML agree → fraud
If ML is uncertain but rules are clean → allow with elevated score

Observability:
   - Prometheus metrics exposed at /metrics
   - Structured JSON logging
   - /health endpoint for Docker healthcheck
"""

import json
import os
import logging
import threading
import time
from collections import defaultdict, deque
from typing import Optional

import numpy as np
import redis
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from pydantic import BaseModel
from sklearn.ensemble import IsolationForest

# ---------------------------------------------------------------------------
# Logging (structured JSON-like output)
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp":"%(asctime)s","level":"%(levelname)s","service":"fraud-service","message":"%(message)s"}',
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(title="Fraud Detection Service", version="1.0.0")

# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------

REQUEST_COUNT = Counter(
    "fraud_requests_total", "Total fraud check requests", ["result", "stage"]
)
REQUEST_LATENCY = Histogram(
    "fraud_request_duration_seconds", "Fraud check latency"
)
FRAUD_SCORE = Histogram(
    "fraud_score_distribution", "Distribution of ML fraud scores",
    buckets=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
)

# ---------------------------------------------------------------------------
# Redis (optional — for velocity tracking across instances)
# ---------------------------------------------------------------------------

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
_redis: Optional[redis.Redis] = None

def get_redis() -> Optional[redis.Redis]:
    global _redis
    if _redis is None:
        try:
            _redis = redis.from_url(REDIS_URL, socket_connect_timeout=2, socket_timeout=2)
            _redis.ping()
            logger.info("Connected to Redis at %s", REDIS_URL)
        except Exception as e:
            logger.warning("Redis unavailable (%s) — falling back to in-process velocity tracking", e)
            _redis = None
    return _redis

# In-process fallback velocity tracker (per user, last 60s request timestamps)
_velocity_store: dict[str, deque] = defaultdict(lambda: deque(maxlen=100))

# ---------------------------------------------------------------------------
# ML Model — IsolationForest trained on synthetic data at startup
# ---------------------------------------------------------------------------

def _train_model() -> IsolationForest:
    """
    Trains an IsolationForest on synthetic transaction data.
    In production this would load a pre-trained model from object storage (S3/GCS).
    Features: [amount, is_high_value, currency_risk]
    """
    rng = np.random.default_rng(42)

    # Normal transactions (90%)
    normal_amounts = rng.lognormal(mean=4.0, sigma=1.5, size=900)  # ~$55 median
    normal_high_value = (normal_amounts > 5000).astype(float)
    normal_currency_risk = rng.uniform(0.0, 0.2, size=900)
    normal = np.column_stack([normal_amounts, normal_high_value, normal_currency_risk])

    # Fraudulent transactions (10%) — extreme amounts, high risk currencies
    fraud_amounts = rng.uniform(9000, 50000, size=100)
    fraud_high_value = np.ones(100)
    fraud_currency_risk = rng.uniform(0.7, 1.0, size=100)
    fraud = np.column_stack([fraud_amounts, fraud_high_value, fraud_currency_risk])

    X = np.vstack([normal, fraud])

    model = IsolationForest(
        n_estimators=100,
        contamination=0.1,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X)
    logger.info("IsolationForest model trained. Features: [amount, is_high_value, currency_risk]")
    return model


# Currency risk scores (0.0 = low risk, 1.0 = high risk)
CURRENCY_RISK: dict[str, float] = {
    "USD": 0.05,
    "EUR": 0.05,
    "GBP": 0.05,
    "INR": 0.10,
    "JPY": 0.10,
    "CAD": 0.05,
    "AUD": 0.08,
}
ALLOWED_CURRENCIES = set(CURRENCY_RISK.keys())
DEFAULT_CURRENCY_RISK = 0.9  # Any unlisted currency = high risk

MODEL: IsolationForest = _train_model()

# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class Transaction(BaseModel):
    payment_id: str
    user_id: str
    amount: float
    currency: str
    correlation_id: Optional[str] = None


class FraudCheckResponse(BaseModel):
    payment_id: str
    fraud: bool
    reason: str
    score: float
    fallback: bool = False
    stage: str  # "rule" | "ml" | "combined"

# ---------------------------------------------------------------------------
# Velocity tracking
# ---------------------------------------------------------------------------

VELOCITY_WINDOW_SECONDS = 60
VELOCITY_THRESHOLD = 10  # max transactions per window


def check_velocity(user_id: str) -> tuple[bool, int]:
    """Returns (exceeded, count)."""
    r = get_redis()
    if r is not None:
        try:
            key = f"velocity:{user_id}"
            count = r.incr(key)
            if count == 1:
                r.expire(key, VELOCITY_WINDOW_SECONDS)
            return int(count) > VELOCITY_THRESHOLD, int(count)
        except Exception as e:
            logger.warning("Redis velocity check failed: %s — using in-process fallback", e)

    # In-process fallback
    now = time.time()
    dq = _velocity_store[user_id]
    # Remove timestamps outside the window
    while dq and now - dq[0] > VELOCITY_WINDOW_SECONDS:
        dq.popleft()
    dq.append(now)
    count = len(dq)
    return count > VELOCITY_THRESHOLD, count

# ---------------------------------------------------------------------------
# ML scoring
# ---------------------------------------------------------------------------

def ml_score(amount: float, currency: str) -> float:
    """Returns a fraud probability [0.0, 1.0]."""
    is_high_value = 1.0 if amount > 5000 else 0.0
    currency_risk = CURRENCY_RISK.get(currency, DEFAULT_CURRENCY_RISK)
    features = np.array([[amount, is_high_value, currency_risk]])

    # IsolationForest returns -1 (outlier) or 1 (inlier)
    # decision_function returns negative values for outliers
    raw_score = MODEL.decision_function(features)[0]

    # Normalise to [0, 1]: more negative raw_score → higher fraud probability
    # raw_score range is approximately [-0.5, 0.5]
    normalised = max(0.0, min(1.0, 0.5 - raw_score))
    return round(normalised, 4)

# ---------------------------------------------------------------------------
# Main scoring endpoint
# ---------------------------------------------------------------------------

@app.post("/score", response_model=FraudCheckResponse)
async def score_transaction(txn: Transaction, request: Request):
    start = time.time()
    correlation_id = txn.correlation_id or request.headers.get("X-Correlation-ID", "unknown")
    logger.info(
        '{"event":"fraud_check_start","payment_id":"%s","user_id":"%s",'
        '"amount":%s,"currency":"%s","correlation_id":"%s"}',
        txn.payment_id, txn.user_id, txn.amount, txn.currency, correlation_id,
    )

    # ----------------------------------------------------------------
    # Stage 1: Rule-based checks
    # ----------------------------------------------------------------
    rule_fraud = False
    rule_reason = "ok"

    if txn.currency not in ALLOWED_CURRENCIES:
        rule_fraud = True
        rule_reason = "unsupported_currency"

    if txn.amount > 10_000:
        rule_fraud = True
        rule_reason = "amount_exceeds_threshold"

    velocity_exceeded, velocity_count = check_velocity(txn.user_id)
    if velocity_exceeded:
        rule_fraud = True
        rule_reason = f"velocity_exceeded:{velocity_count}_in_{VELOCITY_WINDOW_SECONDS}s"

    if rule_fraud:
        REQUEST_COUNT.labels(result="fraud", stage="rule").inc()
        FRAUD_SCORE.observe(0.95)
        REQUEST_LATENCY.observe(time.time() - start)
        logger.warning(
            '{"event":"fraud_detected","stage":"rule","payment_id":"%s","reason":"%s"}',
            txn.payment_id, rule_reason,
        )
        return FraudCheckResponse(
            payment_id=txn.payment_id,
            fraud=True,
            reason=rule_reason,
            score=0.95,
            stage="rule",
        )

    # ----------------------------------------------------------------
    # Stage 2: ML-based scoring
    # ----------------------------------------------------------------
    score = ml_score(txn.amount, txn.currency)
    FRAUD_SCORE.observe(score)

    ml_fraud = score >= 0.75

    stage = "combined" if ml_fraud else "ml"
    result_label = "fraud" if ml_fraud else "clean"
    reason = "ml_high_risk_score" if ml_fraud else "ok"

    REQUEST_COUNT.labels(result=result_label, stage=stage).inc()
    REQUEST_LATENCY.observe(time.time() - start)

    logger.info(
        '{"event":"fraud_check_complete","payment_id":"%s","fraud":%s,"score":%s,"reason":"%s"}',
        txn.payment_id, str(ml_fraud).lower(), score, reason,
    )

    return FraudCheckResponse(
        payment_id=txn.payment_id,
        fraud=ml_fraud,
        reason=reason,
        score=score,
        stage=stage,
    )

# ---------------------------------------------------------------------------
# Health + Metrics
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    return {"status": "ok", "model": "IsolationForest", "version": "1.0.0"}

@app.get("/metrics")
async def metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


# ---------------------------------------------------------------------------
# Kafka pipeline (async fraud.request → fraud.result)
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
FRAUD_REQUEST_TOPIC = "fraud.request"
FRAUD_RESULT_TOPIC  = "fraud.result"
KAFKA_CONSUMER_GROUP = "fraud-service-group"
KAFKA_ENABLED = os.getenv("KAFKA_ENABLED", "true").lower() == "true"

# ---------------------------------------------------------------------------
# Horizontal scaling:
#   Each worker thread creates its OWN Consumer instance (confluent-kafka
#   Consumer is NOT thread-safe and must not be shared).
#   Kafka balances partitions across workers in the same consumer group.
#
#   Scaling rules:
#     - max concurrency = number of partitions on fraud.request (default: 12)
#     - set NUM_WORKERS per pod to parallelise within-process (CPU-bound ML)
#     - add pods to consume more partitions; each pod joins the same group
#     - in-process velocity store becomes per-worker — use Redis for shared
#       velocity tracking in multi-worker / multi-pod deployments
#
#   Hot-user / skewed traffic:
#     When userId partitioning is used, a single high-velocity user sends all
#     events to one partition, starving that partition’s worker of capacity.
#     Mitigation: composite key strategy in Java producer:
#       partitionKey = userId + ":" + (sequenceNum % SALT_FACTOR)
#     This distributes one user’s events across SALT_FACTOR partitions at the
#     cost of losing strict per-user ordering — acceptable for fraud scoring.
# ---------------------------------------------------------------------------
NUM_WORKERS = int(os.getenv("KAFKA_CONSUMER_WORKERS", "3"))


def _run_fraud_decision(
    event_id: str,
    payment_id: str,
    user_id: str,
    amount: float,
    currency: str,
    correlation_id: str,
) -> dict:
    """
    Shared business logic used by both the HTTP handler and the Kafka consumer.
    Returns a dict matching the FraudResultEvent snake_case schema.
    """
    # Stage 1: Rule-based
    if currency not in ALLOWED_CURRENCIES:
        return dict(event_id=event_id, payment_id=payment_id, fraud=True,
                    reason="unsupported_currency", score=0.95, stage="rule",
                    fallback=False, correlation_id=correlation_id)

    if amount > 10_000:
        return dict(event_id=event_id, payment_id=payment_id, fraud=True,
                    reason="amount_exceeds_threshold", score=0.95, stage="rule",
                    fallback=False, correlation_id=correlation_id)

    velocity_exceeded, velocity_count = check_velocity(user_id)
    if velocity_exceeded:
        reason = f"velocity_exceeded:{velocity_count}_in_{VELOCITY_WINDOW_SECONDS}s"
        return dict(event_id=event_id, payment_id=payment_id, fraud=True,
                    reason=reason, score=0.95, stage="rule",
                    fallback=False, correlation_id=correlation_id)

    # Stage 2: ML
    score = ml_score(amount, currency)
    ml_fraud = bool(score >= 0.75)
    return dict(event_id=event_id, payment_id=payment_id,
                fraud=ml_fraud,
                reason="ml_high_risk_score" if ml_fraud else "ok",
                score=float(score),
                stage="combined" if ml_fraud else "ml",
                fallback=False, correlation_id=correlation_id)


def _kafka_consumer_loop() -> None:
    """
    Background daemon thread: consumes fraud.request, publishes fraud.result.
    Uses confluent-kafka's synchronous Consumer API (poll loop).
    """
    try:
        from confluent_kafka import Consumer, Producer, KafkaException
    except ImportError:
        logger.warning("confluent-kafka not installed — Kafka pipeline disabled")
        return

    consumer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "max.poll.interval.ms": 300_000,
        "session.timeout.ms": 30_000,
    }
    producer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "acks": "all",
        "retries": 5,
        "enable.idempotence": True,
        "linger.ms": 5,
    }

    consumer = Consumer(consumer_conf)
    producer = Producer(producer_conf)
    consumer.subscribe([FRAUD_REQUEST_TOPIC])

    logger.info("Kafka fraud pipeline started — consuming from '%s'", FRAUD_REQUEST_TOPIC)

    while True:
        try:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                from confluent_kafka import KafkaError
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("Kafka consumer error: %s", msg.error())
                continue

            raw = msg.value().decode("utf-8")
            payload = json.loads(raw)

            # Extract fields — Java publishes snake_case (FraudRequestEvent)
            event_id      = payload.get("event_id") or payload.get("eventId", "")
            payment_id    = payload.get("payment_id") or payload.get("paymentId", "")
            user_id       = payload.get("user_id") or payload.get("userId", "")
            amount        = float(payload.get("amount", 0))
            currency      = payload.get("currency", "USD")
            correlation_id = payload.get("correlation_id") or payload.get("correlationId", "")

            logger.info(
                '{"event":"kafka_fraud_check","payment_id":"%s","event_id":"%s"}',
                payment_id, event_id,
            )

            result = _run_fraud_decision(
                event_id=event_id,
                payment_id=payment_id,
                user_id=str(user_id),
                amount=amount,
                currency=currency,
                correlation_id=correlation_id,
            )

            result_json = json.dumps(result).encode("utf-8")

            # Propagate all three standard trace headers into fraud.result
            # so FraudResultConsumer can populate MDC without payload parsing.
            trace_id       = ""
            correlation_id = result.get("correlation_id", "")
            headers = [
                ("x-event-id",       event_id.encode()),
                ("x-correlation-id", correlation_id.encode()),
                ("x-trace-id",       trace_id.encode()),  # forwarded from fraud.request header
            ]
            # Attempt to read x-trace-id from incoming Kafka message headers
            if msg.headers():
                for hdr_key, hdr_val in msg.headers():
                    if hdr_key == "x-trace-id" and hdr_val:
                        trace_id = hdr_val.decode("utf-8", errors="replace")
                        headers[2] = ("x-trace-id", trace_id.encode())
                        break

            producer.produce(
                FRAUD_RESULT_TOPIC,
                key=payment_id.encode() if payment_id else None,
                value=result_json,
                headers=headers,
                on_delivery=lambda err, m: logger.error("fraud.result delivery failed: %s", err) if err else None,
            )
            producer.poll(0)  # Trigger delivery callbacks

            consumer.commit(message=msg, asynchronous=False)

            logger.info(
                '{"event":"kafka_fraud_result_published","payment_id":"%s","fraud":%s,"score":%s}',
                payment_id, str(result["fraud"]).lower(), result["score"],
            )

        except Exception as exc:
            logger.error("Unexpected error in Kafka fraud consumer loop: %s", exc, exc_info=True)
            time.sleep(1)  # Brief back-off before retrying


@app.on_event("startup")
async def startup_event() -> None:
    """Start NUM_WORKERS Kafka consumer daemon threads when the FastAPI app starts.

    Each thread creates its own Consumer instance (confluent-kafka Consumer is
    NOT thread-safe). Kafka distributes fraud.request partitions across all
    threads in the consumer group. This enables within-process parallelism
    for CPU-bound ML scoring without needing multiple pods.

    Scaling formula:
        max_parallel_messages = min(NUM_WORKERS, num_partitions_on_fraud.request)

    For production: set NUM_WORKERS=num_partitions and run multiple pods.
    Each pod’s workers participate in the same consumer group — Kafka
    guarantees each partition is assigned to exactly one consumer at a time.
    """
    if KAFKA_ENABLED:
        for i in range(NUM_WORKERS):
            t = threading.Thread(
                target=_kafka_consumer_loop,
                daemon=True,
                name=f"fraud-kafka-consumer-{i}",
            )
            t.start()
        logger.info(
            "Started %d Kafka fraud consumer worker(s) (bootstrap=%s)",
            NUM_WORKERS, KAFKA_BOOTSTRAP,
        )
    else:
        logger.info("Kafka pipeline disabled (KAFKA_ENABLED=false) — HTTP-only mode")

