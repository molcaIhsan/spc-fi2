"""
Streaming multi-pack normalization -- one reading at a time.

Turns a cumulative totalizer weight reading into a per-item unit weight,
dividing out double/triple/quad-pack reads per the machine's own
multi_pack_rules. Takes a LineCalibration instead of one global Config, since
target weight (and therefore plausible multi-pack bins) differs across the
3 machines this job runs.

NOTE: multi_pack_rules is empty for all 3 target hwcodes right now -- nobody
has derived real bins for a ~90g product on these machines yet (the earlier
single-machine research's bins were for a 250g product). Until real bins are
set, every reading is treated as a single pack (divisor=1) -- flagged here,
not silently assumed correct.
"""

from typing import Optional, Tuple

from .config import LineCalibration


class MultiPackNormalizer:
    """Stateful: needs the previous cumulative weight to compute a delta.
    Instantiate one per (hwcode, run_id) -- state must reset on a new run."""

    def __init__(self, calibration: LineCalibration):
        self.cal = calibration
        self._prev_cumulative_weight: Optional[float] = None

    def update(self, cumulative_weight: float) -> Optional[Tuple[float, float, int]]:
        """Returns (raw_delta, unit_weight, pack_count), or None if there's no
        prior reading yet, or the computed unit weight is implausible."""
        prev = self._prev_cumulative_weight
        self._prev_cumulative_weight = cumulative_weight

        if prev is None:
            return None

        raw_delta = cumulative_weight - prev

        for min_w, max_w, divisor in self.cal.multi_pack_rules:
            if min_w <= raw_delta <= max_w:
                unit_weight, pack_count = raw_delta / divisor, int(divisor)
                break
        else:
            unit_weight, pack_count = raw_delta, 1

        if self.cal.valid_weight_min <= unit_weight <= self.cal.valid_weight_max:
            return raw_delta, unit_weight, pack_count
        return None

    def reset(self) -> None:
        """Call at the start of a new run/shift -- there's no valid prior
        reading to diff against across a run boundary."""
        self._prev_cumulative_weight = None
