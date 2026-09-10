from spc.config import LineCalibration
from spc.preprocessing import MultiPackNormalizer


def make_calibration(**overrides) -> LineCalibration:
    defaults = dict(
        target_weight=90.0,
        spec_lower=87.0,
        spec_upper=94.0,
        control_lower=88.5,
        control_upper=91.5,
        valid_weight_min=60.0,
        valid_weight_max=120.0,
        multi_pack_rules=((160.0, 200.0, 2.0), (250.0, 290.0, 3.0)),
    )
    defaults.update(overrides)
    return LineCalibration(**defaults)


def test_first_reading_of_a_run_returns_none():
    norm = MultiPackNormalizer(make_calibration())
    assert norm.update(1000.0) is None


def test_single_pack_reading_passes_through_unchanged():
    norm = MultiPackNormalizer(make_calibration())
    norm.update(1000.0)
    result = norm.update(1090.0)  # delta = 90.0, a plain single item
    assert result == (90.0, 90.0, 1)


def test_double_pack_reading_is_divided_by_two():
    norm = MultiPackNormalizer(make_calibration())
    norm.update(1000.0)
    result = norm.update(1180.0)  # delta = 180.0 -> in the 160-200 double-pack bin
    raw_delta, unit_weight, pack_count = result
    assert raw_delta == 180.0
    assert pack_count == 2
    assert unit_weight == 90.0


def test_implausible_delta_is_discarded():
    norm = MultiPackNormalizer(make_calibration())
    norm.update(1000.0)
    # delta = 20.0 -- not a plausible single item and not a multi-pack bin either
    assert norm.update(1020.0) is None


def test_reset_clears_state_so_next_reading_is_treated_as_first():
    norm = MultiPackNormalizer(make_calibration())
    norm.update(1000.0)
    norm.update(1090.0)
    norm.reset()
    assert norm.update(1180.0) is None  # first reading of the "new run"
