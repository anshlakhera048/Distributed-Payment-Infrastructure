"""
Feature Store Module
====================
Manages real-time feature computation and caching for fraud detection.

Uses Redis for cross-instance feature sharing:
  - Velocity counters: sliding window counts per user
  - Behavioral stats: rolling averages, last transaction timestamps
  - Risk scores: cached fraud rates (updated async from batch pipeline)

Falls back to in-process storage when Redis is unavailable.
"""

import time
import logging
from collections import defaultdict, deque
from typing import Optional

logger = logging.getLogger(__name__)

# Window definitions in seconds
WINDOW_1MIN = 60
WINDOW_5MIN = 300
WINDOW_1HR = 3600


class FeatureStore:
    """
    Real-time feature store backed by Redis with in-process fallback.

    Features are organized into three groups:
      1. Velocity: sliding window transaction counts
      2. Behavioral: rolling averages, timing stats
      3. Risk: cached fraud rates from batch pipeline

    Thread-safety: in-process fallbacks use deque (thread-safe for append/popleft).
    Redis operations are atomic (MULTI/EXEC or Lua scripts).
    """

    def __init__(self, redis_client=None):
        self._redis = redis_client

        # In-process fallback stores
        self._velocity_store: dict[str, deque] = defaultdict(lambda: deque(maxlen=500))
        self._amount_store: dict[str, deque] = defaultdict(lambda: deque(maxlen=500))
        self._last_txn_time: dict[str, float] = {}

    def record_transaction(self, user_id: str, amount: float) -> None:
        """Record a transaction for velocity and behavioral tracking."""
        now = time.time()

        if self._redis is not None:
            try:
                pipe = self._redis.pipeline()
                # Velocity: sorted set with timestamp scores
                vel_key = f"velocity:{user_id}"
                pipe.zadd(vel_key, {f"{now}:{amount}": now})
                pipe.zremrangebyscore(vel_key, 0, now - WINDOW_1HR)
                pipe.expire(vel_key, WINDOW_1HR + 60)

                # Behavioral: amount history
                amt_key = f"amounts:{user_id}"
                pipe.lpush(amt_key, f"{amount}:{now}")
                pipe.ltrim(amt_key, 0, 499)
                pipe.expire(amt_key, WINDOW_1HR + 60)

                # Last transaction time
                pipe.set(f"last_txn:{user_id}", str(now), ex=WINDOW_1HR + 60)

                pipe.execute()
                return
            except Exception as e:
                logger.debug("Redis record_transaction failed: %s — using in-process", e)

        # In-process fallback
        self._velocity_store[user_id].append(now)
        self._amount_store[user_id].append((now, amount))
        self._last_txn_time[user_id] = now

    def get_velocity(self, user_id: str) -> dict[str, int]:
        """Returns transaction counts for 1min, 5min, 1hr windows."""
        now = time.time()

        if self._redis is not None:
            try:
                vel_key = f"velocity:{user_id}"
                count_1min = self._redis.zcount(vel_key, now - WINDOW_1MIN, now)
                count_5min = self._redis.zcount(vel_key, now - WINDOW_5MIN, now)
                count_1hr = self._redis.zcount(vel_key, now - WINDOW_1HR, now)
                return {"1min": int(count_1min), "5min": int(count_5min), "1hr": int(count_1hr)}
            except Exception as e:
                logger.debug("Redis get_velocity failed: %s — using in-process", e)

        # In-process fallback
        dq = self._velocity_store.get(user_id, deque())
        # Clean old entries
        while dq and now - dq[0] > WINDOW_1HR:
            dq.popleft()

        count_1min = sum(1 for t in dq if now - t <= WINDOW_1MIN)
        count_5min = sum(1 for t in dq if now - t <= WINDOW_5MIN)
        count_1hr = len(dq)

        return {"1min": count_1min, "5min": count_5min, "1hr": count_1hr}

    def get_behavioral(self, user_id: str) -> dict[str, float]:
        """Returns behavioral features: avg_amount_1hr, time_since_last."""
        now = time.time()

        if self._redis is not None:
            try:
                amt_key = f"amounts:{user_id}"
                raw_amounts = self._redis.lrange(amt_key, 0, -1)
                amounts = []
                for raw in raw_amounts:
                    parts = raw.decode("utf-8").split(":")
                    if len(parts) >= 2:
                        ts = float(parts[1])
                        if now - ts <= WINDOW_1HR:
                            amounts.append(float(parts[0]))

                avg = sum(amounts) / len(amounts) if amounts else 0.0

                last_raw = self._redis.get(f"last_txn:{user_id}")
                time_since = now - float(last_raw.decode("utf-8")) if last_raw else 0.0

                return {"avg_amount_1hr": avg, "time_since_last": time_since}
            except Exception as e:
                logger.debug("Redis get_behavioral failed: %s — using in-process", e)

        # In-process fallback
        dq = self._amount_store.get(user_id, deque())
        recent = [(t, a) for t, a in dq if now - t <= WINDOW_1HR]
        avg = sum(a for _, a in recent) / len(recent) if recent else 0.0

        last = self._last_txn_time.get(user_id)
        time_since = now - last if last else 0.0

        return {"avg_amount_1hr": avg, "time_since_last": time_since}

    def get_risk_scores(self, user_id: str, merchant_id: str) -> dict[str, float]:
        """Returns cached risk scores. In production, these are populated by a batch pipeline."""
        if self._redis is not None:
            try:
                user_rate = self._redis.get(f"fraud_rate:user:{user_id}")
                merchant_rate = self._redis.get(f"fraud_rate:merchant:{merchant_id}")
                return {
                    "user_fraud_rate": float(user_rate.decode()) if user_rate else 0.0,
                    "merchant_fraud_rate": float(merchant_rate.decode()) if merchant_rate else 0.0,
                }
            except Exception as e:
                logger.debug("Redis get_risk_scores failed: %s", e)

        return {"user_fraud_rate": 0.0, "merchant_fraud_rate": 0.0}
