from spc.models import Label, classify
from spc.config import get_line_for_hwcode


def test_classify_below_spec_is_lsl():
    assert classify(85.0, 87.0, 94.0, False, False) == Label.LSL


def test_classify_below_control_is_lcl():
    assert classify(90.0, 87.0, 94.0, True, False) == Label.LCL


def test_classify_normal():
    assert classify(90.0, 87.0, 94.0, False, False) == Label.NORMAL


def test_classify_above_control_is_ucl():
    assert classify(90.0, 87.0, 94.0, False, True) == Label.UCL


def test_classify_spec_takes_priority_over_control():
    # Above spec AND above control -- spec wins.
    assert classify(95.0, 87.0, 94.0, False, True) == Label.USL


def test_line_mapping_matches_the_poc_machine():
    assert get_line_for_hwcode("ANRITSU64-2") == "8"


def test_line_mapping_still_knows_the_deferred_production_lines():
    # Deferred, not deleted -- see config.py module docstring.
    assert get_line_for_hwcode("ANRITSU54-1") == "1"
    assert get_line_for_hwcode("YAMATO-1") == "2"
    assert get_line_for_hwcode("YAMATO-2") == "3"


def test_line_mapping_unknown_hwcode_returns_none():
    assert get_line_for_hwcode("NOT-A-REAL-HWCODE") is None
