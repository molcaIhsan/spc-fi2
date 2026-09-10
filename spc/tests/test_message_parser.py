from spc.message_parser import parse_message


def test_parses_real_sample_message():
    # Real sample captured during development. action_type='A' (Finish Good
    # count), NOT 'W' (total weight) -- confirms format, not a weight-message shape.
    msg = parse_message("@#YAMATO-12#A#3667#0#1789012526982#4##!")
    assert msg is not None
    assert msg.hwcode == "YAMATO-12"
    assert msg.action_type == "A"
    assert msg.value == 3667.0
    assert msg.quality == 0.0
    assert msg.timestamp == 1789012526982.0
    assert msg.sku_code == "4"
    assert msg.reason is None  # empty string in the raw message -> None


def test_parses_message_with_a_reason():
    msg = parse_message("@#ANRITSU54-1#W#1090#0#1789012526982#4#some_reason#!")
    assert msg.reason == "some_reason"


def test_malformed_message_returns_none():
    assert parse_message("not a valid message") is None


def test_invalid_timestamp_returns_none():
    assert parse_message("@#ANRITSU54-1#W#1090#0#-1#4##!") is None
