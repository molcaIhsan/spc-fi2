from spc.config import Config
from spc.preprocessing import MultiPackNormalizer


def test_first_reading_of_a_run_returns_none():
    norm = MultiPackNormalizer()
    assert norm.update(14460592.0) is None


def test_single_pack_reading_passes_through_unchanged():
    norm = MultiPackNormalizer()
    norm.update(14460000.0)
    result = norm.update(14460250.0)  # delta = 250.0, a plain single item
    assert result == (250.0, 1)


def test_double_pack_reading_is_divided_by_two():
    norm = MultiPackNormalizer()
    norm.update(14460000.0)
    result = norm.update(14460500.0)  # delta = 500.0 -> in the 450-550 double-pack bin
    unit_weight, pack_count = result
    assert pack_count == 2
    assert unit_weight == 250.0


def test_triple_pack_reading_is_divided_by_three():
    norm = MultiPackNormalizer()
    norm.update(14460000.0)
    result = norm.update(14460750.0)  # delta = 750.0 -> in the 700-800 triple-pack bin
    unit_weight, pack_count = result
    assert pack_count == 3
    assert abs(unit_weight - 250.0) < 1e-9


def test_implausible_delta_is_discarded():
    norm = MultiPackNormalizer()
    norm.update(14460000.0)
    # delta = 50.0 -- not a plausible single item and not a multi-pack bin either
    assert norm.update(14460050.0) is None


def test_reset_clears_state_so_next_reading_is_treated_as_first():
    norm = MultiPackNormalizer()
    norm.update(14460000.0)
    norm.update(14460250.0)
    norm.reset()
    assert norm.update(14460500.0) is None  # first reading of the "new run"


def test_config_bins_match_expected_ranges():
    # Locks in the actual calibrated bins -- a change here should be deliberate,
    # not an accidental edit.
    assert Config.MULTI_PACK_RULES == [
        (450.0, 550.0, 2.0),
        (700.0, 800.0, 3.0),
        (950.0, 1050.0, 4.0),
    ]
