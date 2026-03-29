"""
Feature Engineering Module
==========================
Extracts and transforms raw transaction data into ML-ready feature vectors.

Feature groups:
  1. Transaction features: amount, is_high_value, currency_risk
  2. Velocity features: txn_count_1min, txn_count_5min, txn_count_1hr
  3. Behavioral features: avg_amount_1hr, amount_deviation, time_since_last_txn
  4. Aggregated risk: user_fraud_rate_30d, merchant_fraud_rate_30d

All features are designed for real-time extraction (<5ms latency).
"""

import time
import logging
from dataclasses import dataclass, field
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)

# Currency risk scores (0.0 = low risk, 1.0 = high risk)
CURRENCY_RISK: dict[str, float] = {
    "USD": 0.05, "EUR": 0.05, "GBP": 0.05,
    "INR": 0.10, "JPY": 0.10, "CAD": 0.05, "AUD": 0.08,
}
DEFAULT_CURRENCY_RISK = 0.9


@dataclass
class FeatureVector:
    """Extracted feature vector for a single transaction."""
    # Transaction features
    amount: float = 0.0
    is_high_value: float = 0.0
    currency_risk: float = 0.0

    # Velocity features
    txn_count_1min: int = 0
    txn_count_5min: int = 0
    txn_count_1hr: int = 0

    # Behavioral features
    avg_amount_1hr: float = 0.0
    amount_deviation: float = 0.0
    time_since_last_txn: float = 0.0

    # Risk features
    user_fraud_rate_30d: float = 0.0
    merchant_fraud_rate_30d: float = 0.0

    def to_array(self) -> np.ndarray:
        """Returns features as a numpy array for model inference."""
        return np.array([[
            self.amount,
            self.is_high_value,
            self.currency_risk,
            self.txn_count_1min,
            self.txn_count_5min,
            self.txn_count_1hr,
            self.avg_amount_1hr,
            self.amount_deviation,
            self.time_since_last_txn,
            self.user_fraud_rate_30d,
            self.merchant_fraud_rate_30d,
        ]])

    def to_basic_array(self) -> np.ndarray:
        """Returns only the 3 basic features for backward compatibility with v1 model."""
        return np.array([[self.amount, self.is_high_value, self.currency_risk]])


class FeatureExtractor:
    """
    Extracts features from raw transaction data with optional feature store enrichment.

    The extractor is stateless. Velocity and behavioral features require a FeatureStore
    instance; if unavailable, those features default to zero (graceful degradation).
    """

    def __init__(self, feature_store=None):
        self.feature_store = feature_store

    def extract(
        self,
        user_id: str,
        merchant_id: str,
        amount: float,
        currency: str,
    ) -> FeatureVector:
        """Extract full feature vector for a transaction."""
        start = time.time()

        fv = FeatureVector(
            amount=amount,
            is_high_value=1.0 if amount > 5000 else 0.0,
            currency_risk=CURRENCY_RISK.get(currency, DEFAULT_CURRENCY_RISK),
        )

        # Enrich with feature store data if available
        if self.feature_store is not None:
            try:
                velocity = self.feature_store.get_velocity(user_id)
                fv.txn_count_1min = velocity.get("1min", 0)
                fv.txn_count_5min = velocity.get("5min", 0)
                fv.txn_count_1hr = velocity.get("1hr", 0)

                behavioral = self.feature_store.get_behavioral(user_id)
                fv.avg_amount_1hr = behavioral.get("avg_amount_1hr", 0.0)
                fv.time_since_last_txn = behavioral.get("time_since_last", 0.0)

                if fv.avg_amount_1hr > 0:
                    fv.amount_deviation = abs(amount - fv.avg_amount_1hr) / fv.avg_amount_1hr

                risk = self.feature_store.get_risk_scores(user_id, merchant_id)
                fv.user_fraud_rate_30d = risk.get("user_fraud_rate", 0.0)
                fv.merchant_fraud_rate_30d = risk.get("merchant_fraud_rate", 0.0)

            except Exception as e:
                logger.warning("Feature store enrichment failed for user=%s: %s — using basic features",
                    user_id, e)

        elapsed_ms = (time.time() - start) * 1000
        logger.debug("Feature extraction completed in %.1fms for user=%s", elapsed_ms, user_id)

        return fv
