#!/bin/bash
# =============================================================================
# Failure Simulation: Kafka Broker Failure
# =============================================================================
# Simulates Kafka broker becoming unavailable while payments are in-flight.
#
# Expected behavior:
#   - Outbox publisher retries with exponential backoff
#   - API continues accepting payments (writes to outbox table)
#   - No events lost — outbox guarantees delivery after recovery
#   - Consumer reconnects automatically after broker restart
#
# Usage:
#   ./kafka-failure.sh           # pause Kafka for 30s
#   ./kafka-failure.sh 60        # pause Kafka for 60s
#   ./kafka-failure.sh kill      # hard kill + restart
# =============================================================================

set -euo pipefail

DURATION=${1:-30}
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"

echo "=== Kafka Failure Simulation ==="
echo "Duration: ${DURATION}s"
echo ""

if [ "$DURATION" = "kill" ]; then
    echo "[$(date +%T)] Killing Kafka container..."
    docker compose -f "$COMPOSE_FILE" kill kafka
    echo "[$(date +%T)] Kafka killed. Waiting 30s..."
    sleep 30
    echo "[$(date +%T)] Restarting Kafka..."
    docker compose -f "$COMPOSE_FILE" up -d kafka
    echo "[$(date +%T)] Kafka restarted. Waiting for recovery..."
    sleep 15
    echo "[$(date +%T)] Simulation complete."
else
    echo "[$(date +%T)] Pausing Kafka container..."
    docker compose -f "$COMPOSE_FILE" pause kafka
    echo "[$(date +%T)] Kafka paused. Waiting ${DURATION}s..."
    sleep "$DURATION"
    echo "[$(date +%T)] Unpausing Kafka container..."
    docker compose -f "$COMPOSE_FILE" unpause kafka
    echo "[$(date +%T)] Kafka unpaused. Recovery in progress."
fi

echo ""
echo "Verify:"
echo "  1. Check outbox_events table for any FAILED events"
echo "  2. Confirm all PENDING payments eventually reach SUCCESS/FAILED"
echo "  3. Check Prometheus kafka_consumer_lag_total metric"
