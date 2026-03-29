#!/bin/bash
# =============================================================================
# Failure Simulation: Redis Failure
# =============================================================================
# Simulates Redis becoming unavailable.
#
# Expected behavior:
#   - Rate limiter falls back to in-memory Caffeine cache
#   - Payment cache misses handled gracefully (DB fallback)
#   - Idempotency checks still work via DB (Redis is optimization layer)
#   - Fraud service velocity tracking falls back to in-process
#   - No payment loss — Redis is not in the critical path
#
# Usage:
#   ./redis-failure.sh           # pause Redis for 30s
#   ./redis-failure.sh 60        # pause Redis for 60s
# =============================================================================

set -euo pipefail

DURATION=${1:-30}
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"

echo "=== Redis Failure Simulation ==="
echo "Duration: ${DURATION}s"
echo ""

echo "[$(date +%T)] Pausing Redis container..."
docker compose -f "$COMPOSE_FILE" pause redis

echo "[$(date +%T)] Redis paused. Sending test requests..."
echo ""

# Send a batch of requests while Redis is down
for i in $(seq 1 5); do
    IDEMPOTENCY_KEY=$(uuidgen 2>/dev/null || python3 -c "import uuid; print(uuid.uuid4())")
    USER_ID=$(uuidgen 2>/dev/null || python3 -c "import uuid; print(uuid.uuid4())")
    MERCHANT_ID=$(uuidgen 2>/dev/null || python3 -c "import uuid; print(uuid.uuid4())")

    echo "  Request $i (idempotency=$IDEMPOTENCY_KEY)..."
    curl -s -w "  HTTP %{http_code} (%{time_total}s)\n" \
        -X POST http://localhost:8080/payments \
        -H "Content-Type: application/json" \
        -H "Idempotency-Key: $IDEMPOTENCY_KEY" \
        -d "{
            \"userId\": \"$USER_ID\",
            \"merchantId\": \"$MERCHANT_ID\",
            \"amount\": 99.99,
            \"currency\": \"USD\",
            \"description\": \"Redis failure test $i\"
        }" -o /dev/null
done

echo ""
echo "[$(date +%T)] Waiting remaining time... ($((DURATION - 10))s)"
sleep "$((DURATION > 10 ? DURATION - 10 : 1))"

echo "[$(date +%T)] Unpausing Redis container..."
docker compose -f "$COMPOSE_FILE" unpause redis
echo "[$(date +%T)] Redis unpaused."

echo ""
echo "Verify:"
echo "  1. All 5 requests should have returned 201 (fallback active)"
echo "  2. Check logs for 'using in-memory fallback' messages"
echo "  3. Payment cache should be repopulated after Redis recovery"
