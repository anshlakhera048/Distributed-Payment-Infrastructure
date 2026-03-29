#!/bin/bash
# =============================================================================
# Failure Simulation: Fraud Service Failure
# =============================================================================
# Simulates fraud service becoming unavailable.
#
# Expected behavior:
#   - Payment creation still works (fraud check is async via Kafka)
#   - fraud.request events queue up in Kafka
#   - Resilience4j circuit breaker opens after threshold
#   - After recovery: fraud service catches up on queued events
#   - Payments that were waiting get processed (SUCCESS or FRAUD_REJECTED)
#
# Usage:
#   ./fraud-service-failure.sh           # pause for 30s
#   ./fraud-service-failure.sh 60        # pause for 60s
# =============================================================================

set -euo pipefail

DURATION=${1:-30}
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"

echo "=== Fraud Service Failure Simulation ==="
echo "Duration: ${DURATION}s"
echo ""

echo "[$(date +%T)] Pausing fraud-service container..."
docker compose -f "$COMPOSE_FILE" pause fraud-service

echo "[$(date +%T)] Fraud service paused. Creating payments (should succeed, stay PENDING)..."
echo ""

PAYMENT_IDS=()

for i in $(seq 1 5); do
    IDEMPOTENCY_KEY=$(uuidgen 2>/dev/null || python3 -c "import uuid; print(uuid.uuid4())")
    USER_ID=$(uuidgen 2>/dev/null || python3 -c "import uuid; print(uuid.uuid4())")
    MERCHANT_ID=$(uuidgen 2>/dev/null || python3 -c "import uuid; print(uuid.uuid4())")

    RESPONSE=$(curl -s \
        -X POST http://localhost:8080/payments \
        -H "Content-Type: application/json" \
        -H "Idempotency-Key: $IDEMPOTENCY_KEY" \
        -d "{
            \"userId\": \"$USER_ID\",
            \"merchantId\": \"$MERCHANT_ID\",
            \"amount\": 150.00,
            \"currency\": \"USD\",
            \"description\": \"Fraud failure test $i\"
        }")

    PAYMENT_ID=$(echo "$RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('paymentId','?'))" 2>/dev/null || echo "?")
    STATUS=$(echo "$RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status','?'))" 2>/dev/null || echo "?")

    echo "  Payment $i: id=$PAYMENT_ID status=$STATUS"
    PAYMENT_IDS+=("$PAYMENT_ID")
done

echo ""
echo "[$(date +%T)] Waiting ${DURATION}s with fraud service down..."
sleep "$DURATION"

echo "[$(date +%T)] Unpausing fraud-service container..."
docker compose -f "$COMPOSE_FILE" unpause fraud-service

echo "[$(date +%T)] Fraud service resumed. Waiting 15s for catch-up..."
sleep 15

echo ""
echo "Checking payment statuses after recovery:"
for PID in "${PAYMENT_IDS[@]}"; do
    if [ "$PID" != "?" ]; then
        RESPONSE=$(curl -s http://localhost:8080/payments/"$PID")
        STATUS=$(echo "$RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status','?'))" 2>/dev/null || echo "?")
        echo "  $PID → $STATUS"
    fi
done

echo ""
echo "Verify:"
echo "  1. Payments created during outage should now be SUCCESS or FRAUD_REJECTED"
echo "  2. No payment stuck in PENDING after recovery"
echo "  3. Circuit breaker metrics show OPEN→HALF_OPEN→CLOSED transition"
