"""
Sustained-breach k-of-n drift detector -- streaming, one reading at a time.

Decision logic (RESEARCH.md §4.4, §4.7's provisional recommendation):
  1. EWMA and Kalman each estimate the current true weight.
  2. A "breach" = either estimate is outside the calibrated control band
     (target +/- 3-sigma, Config.CONTROL_LOWER/UPPER).
  3. "sustained_drift" = at least SUSTAINED_THRESHOLD breaches in the last
     SUSTAINED_WINDOW readings -- the actual alarm-worthy signal. A single
     breach is noise; sustained_drift is the confirmed drift.

CUSUM is computed (kept for monitoring/future use) but deliberately EXCLUDED
from is_breach: OR-ing it into the trigger, in the actual grid search this
was validated against, collapsed the false-alarm budget (ARL0) from 6,324
items to 360 -- a ~17x more frequent false-alarm rate, for a signal that
"looks" like it should help. Do not add it back to `is_breach` without
re-running that full grid search in alarm-fi2 first (RESEARCH.md's own
caution -- this bug was caught once already porting this exact class).
"""

from collections import deque
from typing import Optional

from .config import Config


class DriftDetector:
    """Instantiate one per (hwcode, sku, run_id) -- state resets at the start
    of every run, matching how EWMA/Kalman are calibrated (initial_state=target)."""

    def __init__(self, config: Optional[Config] = None):
        self.config = config or Config()

        self._ewma = self.config.TARGET_WEIGHT
        self._kalman_x = self.config.TARGET_WEIGHT
        self._kalman_p = 1.0
        self._cusum_pos = 0.0
        self._cusum_neg = 0.0
        self._breach_window: deque = deque(maxlen=self.config.SUSTAINED_WINDOW)

    def update(self, unit_weight: float) -> dict:
        """Feed one new (already multi-pack-normalized) unit weight reading."""
        cfg = self.config

        # EWMA
        self._ewma = cfg.EWMA_LAMBDA * unit_weight + (1.0 - cfg.EWMA_LAMBDA) * self._ewma

        # Kalman: predict + update, single step
        x_pred = self._kalman_x
        p_pred = self._kalman_p + cfg.KALMAN_Q
        residual = unit_weight - x_pred
        k_gain = p_pred / (p_pred + cfg.KALMAN_R)
        self._kalman_x = x_pred + k_gain * residual
        self._kalman_p = (1.0 - k_gain) * p_pred

        # CUSUM -- computed for visibility/future recalibration, NOT part of is_breach.
        self._cusum_pos = max(0.0, self._cusum_pos + (unit_weight - cfg.TARGET_WEIGHT) - cfg.CUSUM_K)
        self._cusum_neg = max(0.0, self._cusum_neg + (cfg.TARGET_WEIGHT - unit_weight) - cfg.CUSUM_K)

        is_breach = (
            self._ewma < cfg.CONTROL_LOWER or self._ewma > cfg.CONTROL_UPPER or
            self._kalman_x < cfg.CONTROL_LOWER or self._kalman_x > cfg.CONTROL_UPPER
        )

        self._breach_window.append(1 if is_breach else 0)
        sustained_drift = (
            len(self._breach_window) == self._breach_window.maxlen and
            sum(self._breach_window) >= cfg.SUSTAINED_THRESHOLD
        )

        return {
            "ewma": self._ewma,
            "kalman": self._kalman_x,
            "cusum_pos": self._cusum_pos,   # informational only -- see module docstring
            "cusum_neg": self._cusum_neg,   # informational only -- see module docstring
            "is_breach": is_breach,
            "sustained_drift": sustained_drift,
        }
