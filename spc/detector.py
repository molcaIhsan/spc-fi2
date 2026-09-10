"""
Sustained-breach k-of-n drift detector -- streaming, one reading at a time.

Decision logic (RESEARCH.md §4.4, §4.7's provisional recommendation, from the
separate alarm-fi2 research repo -- validated there for ANRITSU64-2, NOT for
the 3 machines this job actually targets):
  1. EWMA and Kalman each estimate the current true weight.
  2. A "breach" = either estimate is outside the calibrated control band
     (target +/- 3-sigma).
  3. "sustained_drift"/alarm = at least sustained_threshold breaches in the
     last sustained_window readings -- the actual alarm-worthy signal. A
     single breach is noise; sustained_drift is the confirmed drift.

CUSUM is computed (kept for monitoring/future use) but deliberately EXCLUDED
from is_breach: OR-ing it into the trigger, in the actual grid search this
was validated against (for ANRITSU64-2), collapsed the false-alarm budget
(ARL0) from 6,324 items to 360 -- a ~17x more frequent false-alarm rate, for
a signal that "looks" like it should help. Do not add it back to `is_breach`
without re-running that full grid search first.

Takes a LineCalibration (config.py) instead of one global Config, since this
job runs 3 different machines with 3 different targets at once.
"""

from collections import deque

from .config import LineCalibration
from .models import Label, classify


class DriftDetector:
    """Instantiate one per (hwcode, run_id) -- state resets at the start of
    every run, matching how EWMA/Kalman are calibrated (initial_state=target)."""

    def __init__(self, calibration: LineCalibration):
        self.cal = calibration

        self._ewma = calibration.target_weight
        self._kalman_x = calibration.target_weight
        self._kalman_p = 1.0
        self._cusum_pos = 0.0
        self._cusum_neg = 0.0
        self._breach_window: deque = deque(maxlen=calibration.sustained_window)

    def update(self, unit_weight: float) -> dict:
        """Feed one new (already multi-pack-normalized) unit weight reading."""
        cal = self.cal

        # EWMA
        self._ewma = cal.ewma_lambda * unit_weight + (1.0 - cal.ewma_lambda) * self._ewma

        # Kalman: predict + update, single step
        x_pred = self._kalman_x
        p_pred = self._kalman_p + cal.kalman_q
        residual = unit_weight - x_pred
        k_gain = p_pred / (p_pred + cal.kalman_r)
        self._kalman_x = x_pred + k_gain * residual
        self._kalman_p = (1.0 - k_gain) * p_pred

        # CUSUM -- informational only, see module docstring.
        self._cusum_pos = max(0.0, self._cusum_pos + (unit_weight - cal.target_weight) - 0.5)
        self._cusum_neg = max(0.0, self._cusum_neg + (cal.target_weight - unit_weight) - 0.5)

        below_control = self._ewma < cal.control_lower or self._kalman_x < cal.control_lower
        above_control = self._ewma > cal.control_upper or self._kalman_x > cal.control_upper
        is_breach = below_control or above_control

        self._breach_window.append(1 if is_breach else 0)
        sustained_drift = (
            len(self._breach_window) == self._breach_window.maxlen and
            sum(self._breach_window) >= cal.sustained_threshold
        )

        label = classify(
            unit_weight=unit_weight,
            spec_lower=cal.spec_lower,
            spec_upper=cal.spec_upper,
            below_control=below_control,
            above_control=above_control,
        )

        return {
            "ewma": self._ewma,
            "kalman": self._kalman_x,
            "cusum_pos": self._cusum_pos,   # informational only -- see module docstring
            "cusum_neg": self._cusum_neg,   # informational only -- see module docstring
            "is_breach": is_breach,
            "sustained_drift": sustained_drift,   # column name is just "alarm" -- k/n are tunable
            "label": label.value,
        }
