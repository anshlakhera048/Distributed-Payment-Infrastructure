"""
Model Manager Module
====================
Manages ML model lifecycle: training, versioning, serving, and hot-reloading.

Architecture:
  - Models are versioned with a semantic version string (e.g., "v2.1")
  - Active model can be swapped at runtime via /admin/model/reload endpoint
  - Model metadata (version, training timestamp, metrics) tracked in-memory
  - In production: models stored in object storage (S3/GCS), loaded on startup

Model versions:
  - v1: IsolationForest with 3 features (amount, is_high_value, currency_risk)
  - v2: IsolationForest with 11 features (full feature engineering pipeline)

The manager provides backward-compatible scoring: if the active model expects
3 features, FeatureVector.to_basic_array() is used; otherwise to_array().
"""

import time
import logging
import threading
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
from sklearn.ensemble import IsolationForest

logger = logging.getLogger(__name__)


@dataclass
class ModelMetadata:
    """Tracks model provenance and performance metrics."""
    version: str
    feature_count: int
    trained_at: float = field(default_factory=time.time)
    training_samples: int = 0
    contamination: float = 0.1
    auc_roc: Optional[float] = None
    precision: Optional[float] = None
    recall: Optional[float] = None


class ModelManager:
    """
    Thread-safe ML model manager with versioning and hot-reload support.

    The active model is protected by a read-write lock pattern:
      - Scoring (read path): lock-free via atomic reference swap
      - Model reload (write path): trains new model, then atomically swaps

    Usage:
        manager = ModelManager()
        score = manager.score(feature_vector)
        manager.reload_model(version="v2.1", training_data=X)
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._model: Optional[IsolationForest] = None
        self._metadata: Optional[ModelMetadata] = None
        self._train_default_model()

    def _train_default_model(self) -> None:
        """Train the default v1 model (3 features) on synthetic data."""
        rng = np.random.default_rng(42)

        normal_amounts = rng.lognormal(mean=4.0, sigma=1.5, size=900)
        normal_high_value = (normal_amounts > 5000).astype(float)
        normal_currency_risk = rng.uniform(0.0, 0.2, size=900)
        normal = np.column_stack([normal_amounts, normal_high_value, normal_currency_risk])

        fraud_amounts = rng.uniform(9000, 50000, size=100)
        fraud_high_value = np.ones(100)
        fraud_currency_risk = rng.uniform(0.7, 1.0, size=100)
        fraud = np.column_stack([fraud_amounts, fraud_high_value, fraud_currency_risk])

        X = np.vstack([normal, fraud])

        model = IsolationForest(
            n_estimators=100, contamination=0.1, random_state=42, n_jobs=-1,
        )
        model.fit(X)

        self._model = model
        self._metadata = ModelMetadata(
            version="v1.0",
            feature_count=3,
            training_samples=len(X),
            contamination=0.1,
        )
        logger.info("Default model v1.0 trained: %d samples, %d features",
                     len(X), 3)

    def train_v2_model(self, feature_data: Optional[np.ndarray] = None) -> None:
        """
        Train v2 model with enhanced 11-feature pipeline.
        If no training data provided, generates synthetic data.
        """
        rng = np.random.default_rng(42)

        if feature_data is None:
            n_normal, n_fraud = 900, 100

            # Generate synthetic 11-feature data
            # [amount, is_high_value, currency_risk, txn_1min, txn_5min, txn_1hr,
            #  avg_amount_1hr, amount_deviation, time_since_last, user_fraud_rate, merchant_fraud_rate]
            normal = np.column_stack([
                rng.lognormal(4.0, 1.5, n_normal),           # amount
                (rng.lognormal(4.0, 1.5, n_normal) > 5000).astype(float),  # is_high_value
                rng.uniform(0.0, 0.2, n_normal),             # currency_risk
                rng.poisson(2, n_normal),                    # txn_count_1min
                rng.poisson(5, n_normal),                    # txn_count_5min
                rng.poisson(15, n_normal),                   # txn_count_1hr
                rng.lognormal(4.0, 1.0, n_normal),           # avg_amount_1hr
                rng.uniform(0.0, 0.5, n_normal),             # amount_deviation
                rng.exponential(300, n_normal),               # time_since_last
                rng.uniform(0.0, 0.05, n_normal),            # user_fraud_rate
                rng.uniform(0.0, 0.03, n_normal),            # merchant_fraud_rate
            ])

            fraud = np.column_stack([
                rng.uniform(9000, 50000, n_fraud),            # amount
                np.ones(n_fraud),                             # is_high_value
                rng.uniform(0.7, 1.0, n_fraud),              # currency_risk
                rng.poisson(12, n_fraud),                     # txn_count_1min (high velocity)
                rng.poisson(30, n_fraud),                     # txn_count_5min
                rng.poisson(50, n_fraud),                     # txn_count_1hr
                rng.uniform(100, 500, n_fraud),               # avg_amount_1hr
                rng.uniform(2.0, 10.0, n_fraud),              # amount_deviation (high)
                rng.uniform(0, 5, n_fraud),                   # time_since_last (rapid fire)
                rng.uniform(0.1, 0.5, n_fraud),               # user_fraud_rate
                rng.uniform(0.05, 0.3, n_fraud),              # merchant_fraud_rate
            ])

            feature_data = np.vstack([normal, fraud])

        model = IsolationForest(
            n_estimators=200, contamination=0.1, random_state=42, n_jobs=-1,
        )
        model.fit(feature_data)

        with self._lock:
            self._model = model
            self._metadata = ModelMetadata(
                version="v2.0",
                feature_count=11,
                training_samples=len(feature_data),
                contamination=0.1,
            )

        logger.info("Model v2.0 trained: %d samples, %d features",
                     len(feature_data), 11)

    def score(self, features: np.ndarray) -> float:
        """
        Score a feature vector. Returns fraud probability [0.0, 1.0].

        Handles feature count mismatch gracefully:
          - If model expects 3 features and 11 are provided → use first 3
          - If model expects 11 features and 3 are provided → pad with zeros
        """
        model = self._model
        metadata = self._metadata

        if model is None:
            logger.warning("No model loaded — returning default score 0.5")
            return 0.5

        expected = metadata.feature_count if metadata else features.shape[1]

        if features.shape[1] != expected:
            if features.shape[1] > expected:
                features = features[:, :expected]
            else:
                padding = np.zeros((features.shape[0], expected - features.shape[1]))
                features = np.hstack([features, padding])

        raw_score = model.decision_function(features)[0]
        normalised = max(0.0, min(1.0, 0.5 - raw_score))
        return round(normalised, 4)

    def get_metadata(self) -> Optional[dict]:
        """Returns current model metadata as dict."""
        if self._metadata is None:
            return None
        return {
            "version": self._metadata.version,
            "feature_count": self._metadata.feature_count,
            "trained_at": self._metadata.trained_at,
            "training_samples": self._metadata.training_samples,
            "contamination": self._metadata.contamination,
        }

    @property
    def version(self) -> str:
        return self._metadata.version if self._metadata else "unknown"

    @property
    def feature_count(self) -> int:
        return self._metadata.feature_count if self._metadata else 0
