"""Status label combining spec-limit (actual defect) and control-limit
(early-warning) checks into one field."""

from enum import Enum


class Label(str, Enum):
    """Order LSL < LCL < NORMAL < UCL < USL is deliberate: low-to-high across
    the weight axis, not alphabetical or severity-only. Spec-limit checks take
    priority over control-limit ones -- see classify()."""

    LSL = "LSL"        # unit_weight below spec_lower -- real reject
    LCL = "LCL"        # estimator below control_lower -- early warning, not yet a reject
    NORMAL = "Normal"
    UCL = "UCL"        # estimator above control_upper -- early warning
    USL = "USL"        # unit_weight above spec_upper -- real reject


def classify(
    unit_weight: float,
    spec_lower: float,
    spec_upper: float,
    below_control: bool,
    above_control: bool,
) -> Label:
    """below_control/above_control: OR across every estimator in play (EWMA
    and/or Kalman) -- matches DriftDetector.is_breach's own OR logic exactly,
    rather than picking just one estimator for the label."""
    if unit_weight < spec_lower:
        return Label.LSL
    if unit_weight > spec_upper:
        return Label.USL
    if below_control:
        return Label.LCL
    if above_control:
        return Label.UCL
    return Label.NORMAL
