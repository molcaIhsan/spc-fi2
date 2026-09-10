from spc.config import LineCalibration
from spc.detector import DriftDetector


def make_calibration(**overrides) -> LineCalibration:
    defaults = dict(
        target_weight=90.0,
        spec_lower=87.0,
        spec_upper=94.0,
        control_lower=88.5,
        control_upper=91.5,
        sustained_threshold=9,
        sustained_window=40,
    )
    defaults.update(overrides)
    return LineCalibration(**defaults)


def test_initial_state_is_target_and_no_breach():
    cal = make_calibration()
    det = DriftDetector(cal)
    result = det.update(cal.target_weight)
    assert result["ewma"] == cal.target_weight
    assert result["is_breach"] is False
    assert result["sustained_drift"] is False
    assert result["label"] == "Normal"


def test_cusum_exceeding_its_threshold_does_not_trigger_is_breach():
    """
    Regression test for the exact bug caught while porting this class: an
    earlier version OR'd cusum_breach into is_breach, which the real grid
    search proved collapses the false-alarm budget (RESEARCH.md §4.4:
    ARL0 6,324 -> 360, for the separate ANRITSU64-2 research). CUSUM must
    stay informational only.

    Uses a +1.0g bias, not +0.5g: detector.py's CUSUM slack constant is
    hardcoded to 0.5, so a +0.5g bias exactly cancels it and cusum_pos never
    accumulates at all -- caught by actually running this test.
    """
    cal = make_calibration()
    det = DriftDetector(cal)
    cusum_exceeded_while_in_band = False

    for _ in range(30):
        result = det.update(cal.target_weight + 1.0)
        in_band = cal.control_lower <= result["ewma"] <= cal.control_upper
        cusum_over = result["cusum_pos"] > 1.5
        if cusum_over and in_band:
            cusum_exceeded_while_in_band = True
            assert result["is_breach"] is False, (
                "CUSUM exceeded its threshold while EWMA was still in-band, "
                "but is_breach was True -- CUSUM has leaked into the trigger."
            )

    assert cusum_exceeded_while_in_band, "test setup didn't actually exercise the CUSUM-over/in-band case"


def test_single_breach_does_not_trigger_sustained_drift():
    cal = make_calibration()
    det = DriftDetector(cal)
    result = det.update(200.0)  # far outside any plausible band
    assert result["is_breach"] is True
    assert result["sustained_drift"] is False


def test_sustained_drift_fires_once_threshold_breaches_fill_the_window():
    cal = make_calibration()
    det = DriftDetector(cal)
    n, k = cal.sustained_window, cal.sustained_threshold

    for _ in range(n - k):
        result = det.update(cal.target_weight)
        assert result["sustained_drift"] is False

    fired = False
    for _ in range(k):
        result = det.update(200.0)
        if result["sustained_drift"]:
            fired = True
    assert fired, f"expected sustained_drift to fire after {k} breaches within a window of {n}"


def test_sustained_drift_cannot_fire_before_the_window_is_full():
    cal = make_calibration()
    det = DriftDetector(cal)
    for i in range(cal.sustained_window - 1):
        result = det.update(200.0)  # every single reading is a breach
        assert result["sustained_drift"] is False, f"fired early at step {i}, before window filled"


def test_label_priority_spec_over_control():
    cal = make_calibration()
    det = DriftDetector(cal)
    result = det.update(cal.spec_upper + 1.0)  # above spec -> USL, even though also above control
    assert result["label"] == "USL"
