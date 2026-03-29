#!/bin/bash
# =============================================================================
# Failure Simulation: Database (PostgreSQL) Failure
# =============================================================================
# Simulates PostgreSQL becoming unavailable — the most critical failure.
#
# Expected behavior:
#   - New payment creation fails with 500 (DB is in critical path)
#   - Kafka consumers pause processing (DB writes fail)
#   - Spring Retry handles transient failures automatically
#   - After recovery: outbox publisher catches up, consumers resume
#   - No data corruption — all operations are transactional
#
# Usage:
#   ./database-failure.sh           # pause PG for 15s
#   ./database-failure.sh 30        # pause PG for 30s
# =============================================================================

set -euo pipefail

DURATION=${1:-15}
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"

echo "=== Database Failure Simulation ==="
echo "Duration: ${DURATION}s"
echo "WARNING: This WILL cause request failures. This is expected."
echo ""

echo "[$(date +%T)] Pausing PostgreSQL container..."
docker compose -f "$COMPOSE_FILE" pause postgres

echo "[$(date +%T)] PostgreSQL paused. Sending test requests (expect failures)..."
echo ""

for i in $(seq 1 3); do
    IDEMPOTENCY_KEY=$(uuidgen 2>/dev/null || python3 -c "import uuid; print(uuid.uuid4())")
    USER_ID=$(uuidgen 2>/dev/null || python3 -c "import uuid; print(uuid.uuid4())")
    MERCHANT_ID=$(uuidgen 2>/dev/null || python3 -c "import uuid; print(uuid.uuid4())")

    echo "  Request $i (expect 500)..."
    HTTP_CODE=$(curl -s -w "%{http_code}" \
        -X POST http://localhost:8080/payments \
        -H "Content-Type: application/json" \
        -H "Idempotency-Key: $IDEMPOTENCY_KEY" \
        -d "{
            \"userId\": \"$USER_ID\",
            \"merchantId\": \"$MERCHANT_ID\",
            \"amount\": 49.99,
            \"currency\": \"USD\",
            \"description\": \"DB failure test $i\"
        }" -o /dev/null)

    echo "  → HTTP $HTTP_CODE"
done

echo ""
echo "[$(date +%T)] Waiting ${DURATION}s..."
sleep "$DURATION"

echo "[$(date +%T)] Unpausing PostgreSQL container..."
docker compose -f "$COMPOSE_FILE" unpause postgres
echo "[$(date +%T)] PostgreSQL unpaused. Waiting for HikariCP reconnection (10s)..."
sleep 10

echo ""
echo "Post-recovery test..."
IDEMPOTENCY_KEY=$(uuidgen 2>/dev/null || python3 -c "import uuid; print(uuid.uuid4())")
USER_ID=$(uuidgen 2>/dev/null || python3 -c "import uuid; print(uuid.uuid4())")
MERCHANT_ID=$(uuidgen 2>/dev/null || python3 -c "import uuid; print(uuid.uuid4())")

HTTP_CODE=$(curl -s -w "%{http_code}" \
    -X POST http://localhost:8080/payments \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: $IDEMPOTENCY_KEY" \
    -d "{
        \"userId\": \"$USER_ID\",
        \"merchantId\": \"$MERCHANT_ID\",
        \"amount\": 25.00,
        \"currency\": \"USD\",
        \"description\": \"Post-recovery test\"
    }" -o /dev/null)

echo "  Recovery test → HTTP $HTTP_CODE (expect 201)"
echo ""
echo "Verify:"
echo "  1. Requests during outage returned 500"
echo "  2. Post-recovery request returned 201"
echo "  3. No orphaned PENDING payments (reconciliation will catch them)"
echo "  4. Kafka consumer lag recovered to 0"
