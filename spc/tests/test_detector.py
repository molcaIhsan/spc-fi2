from spc.config import Config
from spc.detector import DriftDetector


def test_initial_state_is_target_and_no_breach():
    det = DriftDetector()
    result = det.update(Config.TARGET_WEIGHT)
    assert result["ewma"] == Config.TARGET_WEIGHT
    assert result["is_breach"] is False
    assert result["sustained_drift"] is False


def test_cusum_exceeding_its_threshold_does_not_trigger_is_breach():
    """
    Regression test for the exact bug caught while porting this class from
    alarm-fi2: an earlier version OR'd cusum_breach into is_breach, which the
    real grid search proved collapses the false-alarm budget (RESEARCH.md
    §4.4: ARL0 6,324 -> 360). CUSUM must stay informational only.

    A small, sustained +0.5g bias accumulates CUSUM past CUSUM_H well before
    EWMA/Kalman (much slower-moving) leave the control band -- if this ever
    starts asserting is_breach=True while ewma/kalman are still in-band, CUSUM
    has been wired back into the trigger by mistake.
    """
    det = DriftDetector()
    cusum_exceeded_while_in_band = False

    for _ in range(30):
        result = det.update(Config.TARGET_WEIGHT + 0.5)
        in_band = Config.CONTROL_LOWER <= result["ewma"] <= Config.CONTROL_UPPER
        cusum_over = result["cusum_pos"] > Config.CUSUM_H
        if cusum_over and in_band:
            cusum_exceeded_while_in_band = True
            assert result["is_breach"] is False, (
                "CUSUM exceeded its threshold while EWMA was still in-band, "
                "but is_breach was True -- CUSUM has leaked into the trigger."
            )

    assert cusum_exceeded_while_in_band, "test setup didn't actually exercise the CUSUM-over/in-band case"


def test_single_breach_does_not_trigger_sustained_drift():
    det = DriftDetector()
    # One extreme reading is one breach, not a sustained drift.
    result = det.update(300.0)
    assert result["is_breach"] is True
    assert result["sustained_drift"] is False


def test_sustained_drift_fires_once_threshold_breaches_fill_the_window():
    det = DriftDetector()
    n, k = Config.SUSTAINED_WINDOW, Config.SUSTAINED_THRESHOLD

    # Fill the window with (n - k) in-control readings, then k extreme ones.
    for _ in range(n - k):
        result = det.update(Config.TARGET_WEIGHT)
        assert result["sustained_drift"] is False

    fired = False
    for _ in range(k):
        result = det.update(300.0)
        if result["sustained_drift"]:
            fired = True
    assert fired, f"expected sustained_drift to fire after {k} breaches within a window of {n}"


def test_sustained_drift_cannot_fire_before_the_window_is_full():
    det = DriftDetector()
    for i in range(Config.SUSTAINED_WINDOW - 1):
        result = det.update(300.0)  # every single reading is a breach
        assert result["sustained_drift"] is False, f"fired early at step {i}, before window filled"
