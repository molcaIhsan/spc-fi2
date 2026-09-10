"""
Streaming multi-pack normalization -- one reading at a time.

Turns a cumulative totalizer weight reading into a per-item unit weight,
dividing out double/triple/quad-pack reads per Config.MULTI_PACK_RULES.
Ported from alarm-fi2 src/preprocessing.py (batch/vectorized there; this is
the same logic applied to a single new reading, for streaming use).
"""

from typing import Optional, Tuple

from .config import Config


class MultiPackNormalizer:
    """Stateful: needs the previous cumulative weight to compute a delta.
    Instantiate one per run/session (state must reset when a new run starts)."""

    def __init__(self, config: Optional[Config] = None):
        self.config = config or Config()
        self._prev_cumulative_weight: Optional[float] = None

    def update(self, cumulative_weight: float) -> Optional[Tuple[float, int]]:
        """
        Feed the next raw cumulative (totalizer) weight reading.
        Returns (unit_weight, pack_count), or None if there's no prior reading
        yet (first item of a run) or the computed unit weight is outside the
        plausible single-pack range (discarded as noise/fault, matching the
        offline pipeline's VALID_WEIGHT_MIN/MAX filter).
        """
        prev = self._prev_cumulative_weight
        self._prev_cumulative_weight = cumulative_weight

        if prev is None:
            return None

        raw_delta = cumulative_weight - prev

        for min_w, max_w, divisor in self.config.MULTI_PACK_RULES:
            if min_w <= raw_delta <= max_w:
                unit_weight, pack_count = raw_delta / divisor, int(divisor)
                break
        else:
            unit_weight, pack_count = raw_delta, 1

        if self.config.VALID_WEIGHT_MIN <= unit_weight <= self.config.VALID_WEIGHT_MAX:
            return unit_weight, pack_count
        return None

    def reset(self) -> None:
        """Call at the start of a new run/shift -- there's no valid prior
        reading to diff against across a run boundary."""
        self._prev_cumulative_weight = None
