#!/bin/bash
# =============================================================================
# Failure Simulation: Network Partition
# =============================================================================
# Simulates a network partition between services using Docker network disconnect.
#
# Expected behavior:
#   - Services that can't reach dependencies use fallback paths
#   - Circuit breakers open and provide fallback responses
#   - After partition heals: services reconnect automatically
#
# Usage:
#   ./network-partition.sh                    # isolate fraud-service for 20s
#   ./network-partition.sh payment-service 30 # isolate payment-service for 30s
# =============================================================================

set -euo pipefail

TARGET=${1:-fraud-service}
DURATION=${2:-20}
NETWORK="payments-system_default"

echo "=== Network Partition Simulation ==="
echo "Target: $TARGET"
echo "Duration: ${DURATION}s"
echo "Network: $NETWORK"
echo ""

echo "[$(date +%T)] Disconnecting $TARGET from network..."
docker network disconnect "$NETWORK" "$TARGET" 2>/dev/null || \
    docker network disconnect "${NETWORK}" "payments-system-${TARGET}-1" 2>/dev/null || \
    echo "Warning: Could not disconnect. Check container/network names."

echo "[$(date +%T)] $TARGET isolated. Waiting ${DURATION}s..."
sleep "$DURATION"

echo "[$(date +%T)] Reconnecting $TARGET to network..."
docker network connect "$NETWORK" "$TARGET" 2>/dev/null || \
    docker network connect "${NETWORK}" "payments-system-${TARGET}-1" 2>/dev/null || \
    echo "Warning: Could not reconnect. Check container/network names."

echo "[$(date +%T)] Network partition healed."
echo ""
echo "Verify:"
echo "  1. Check circuit breaker state in actuator"
echo "  2. Verify service health at /actuator/health"
echo "  3. Check for timeout/connection errors in logs"
