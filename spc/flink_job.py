"""
SPC monitoring job -- standalone, no dependency on ajinomoto-etl-flink.

Consumes FI2.data from Kafka (own consumer group), filters to
action_type == 'W' (total weight) for the 3 target machines (ANRITSU54-1 /
YAMATO-1 / YAMATO-2 -- Lines 1/2/3's checkweighers), runs the sustained-breach
k-of-n drift detector per machine, writes one row per reading to Postgres
(ajinomoto_mes.spc_readings).

IMPORTANT, NOT YET DONE: none of these 3 lines have a real Phase I baseline.
config.LINE_CALIBRATIONS are placeholders (YAMATO-1/YAMATO-2 explicitly share
ANRITSU54-1's numbers, per instruction -- ASSUMED, not independently
confirmed). This job can run in a "collect data, don't act on the alarms yet"
mode today.

Run boundary: the raw message has no run_id. This job resets detector state
on a SKU change per hwcode and synthesizes its own run_id
("<hwcode>_<sku>_<first-seen-timestamp>") for grouping/audit.

REQUIRES VERIFICATION AGAINST YOUR ACTUAL FLINK CLUSTER VERSION before
deploying -- see Dockerfile.
"""

import logging
import sys
from datetime import datetime, timezone
from typing import Dict

from pyflink.common import Row, WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.common import Time
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor, StateTtlConfig
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema

from .config import AppConfig, DEFAULT_CONFIG, TARGET_HWCODES, LineCalibration, fetch_line_calibrations, get_line_for_hwcode
from .message_parser import parse_message, ParsedMessage
from .preprocessing import MultiPackNormalizer
from .detector import DriftDetector
from .sinks import PostgresSinkBuilder


def _to_utc_datetime(timestamp_ms: float) -> datetime:
    return datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)


def filter_spc_weight_data(record: ParsedMessage) -> bool:
    return (
        record is not None
        and record.action_type == "W"
        and record.hwcode in TARGET_HWCODES
    )


class SpcDetectorFunction(KeyedProcessFunction):
    """Keyed by hwcode. Resets on SKU change (see module docstring's run-
    boundary note) rather than a true run_id, which the raw stream lacks.

    Takes calibrations as a constructor arg (loaded once in main(), from
    Postgres if reachable) rather than reading a module-level dict -- this
    function gets pickled and shipped to TaskManager processes, which may not
    share process state with wherever main() ran."""

    def __init__(self, calibrations: Dict[str, LineCalibration], state_ttl_hours: int):
        self.calibrations = calibrations
        self.state_ttl_hours = state_ttl_hours

    def open(self, runtime_context):
        ttl_config = StateTtlConfig.new_builder(Time.hours(self.state_ttl_hours)).build()

        normalizer_desc = ValueStateDescriptor("normalizer", Types.PICKLED_BYTE_ARRAY())
        normalizer_desc.enable_time_to_live(ttl_config)
        self.normalizer_state = runtime_context.get_state(normalizer_desc)

        detector_desc = ValueStateDescriptor("detector", Types.PICKLED_BYTE_ARRAY())
        detector_desc.enable_time_to_live(ttl_config)
        self.detector_state = runtime_context.get_state(detector_desc)

        run_desc = ValueStateDescriptor("run_context", Types.PICKLED_BYTE_ARRAY())
        run_desc.enable_time_to_live(ttl_config)
        self.run_state = runtime_context.get_state(run_desc)  # (sku, run_id)

    def process_element(self, record: ParsedMessage, ctx):
        hwcode = record.hwcode
        calibration = self.calibrations[hwcode]

        run_context = self.run_state.value()
        is_new_run = run_context is None or run_context[0] != record.sku_code
        if is_new_run:
            run_id = f"{hwcode}_{record.sku_code}_{int(record.timestamp)}"
            self.run_state.update((record.sku_code, run_id))
            normalizer = MultiPackNormalizer(calibration)
            detector = DriftDetector(calibration)
        else:
            run_id = run_context[1]
            normalizer = self.normalizer_state.value() or MultiPackNormalizer(calibration)
            detector = self.detector_state.value() or DriftDetector(calibration)

        norm_result = normalizer.update(record.value)
        self.normalizer_state.update(normalizer)

        if norm_result is None:
            return  # first reading of this run, or an implausible delta -- discarded

        raw_delta, unit_weight, pack_count = norm_result

        result = detector.update(unit_weight)
        self.detector_state.update(detector)

        yield {
            "machine_name": hwcode,
            "line": get_line_for_hwcode(hwcode),
            "sku": record.sku_code,
            "run_id": run_id,
            "event_timestamp": _to_utc_datetime(record.timestamp),
            "raw_data": record.value,
            "raw_delta": raw_delta,
            "unit_weight": unit_weight,
            "pack_count": pack_count,
            "ewma": result["ewma"],
            "kalman": result["kalman"],
            "is_breach": result["is_breach"],
            "label": result["label"],
            "alarm": result["sustained_drift"],
            "config_status": calibration.status,
        }


SPC_READING_FIELD_TYPES = [
    Types.STRING(),   # machine_name
    Types.STRING(),   # line
    Types.STRING(),   # sku
    Types.STRING(),   # run_id
    Types.SQL_TIMESTAMP(),  # event_timestamp
    Types.DOUBLE(),   # raw_data
    Types.DOUBLE(),   # raw_delta
    Types.DOUBLE(),   # unit_weight
    Types.INT(),      # pack_count
    Types.DOUBLE(),   # ewma
    Types.DOUBLE(),   # kalman
    Types.BOOLEAN(),  # is_breach
    Types.STRING(),   # label
    Types.BOOLEAN(),  # alarm
    Types.STRING(),   # config_status
]

SPC_READINGS_INSERT_SQL = """
    INSERT INTO ajinomoto_mes.spc_readings
    (machine_name, line, sku, run_id, event_timestamp, raw_data, raw_delta,
     unit_weight, pack_count, ewma, kalman, is_breach, label, alarm, config_status)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


def _to_row(d: dict) -> Row:
    return Row(
        d["machine_name"], d["line"], d["sku"], d["run_id"], d["event_timestamp"],
        d["raw_data"], d["raw_delta"], d["unit_weight"], d["pack_count"],
        d["ewma"], d["kalman"], d["is_breach"], d["label"], d["alarm"],
        d["config_status"],
    )


def build_job(config: AppConfig, calibrations: Dict[str, LineCalibration]) -> StreamExecutionEnvironment:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.get_checkpoint_config().set_checkpoint_interval(config.flink.checkpoint_interval)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(config.kafka.bootstrap_server)
        .set_topics(config.kafka.topic)
        .set_group_id(config.kafka.group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    raw_stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka FI2.data")

    parsed = (
        raw_stream.map(parse_message, output_type=Types.PICKLED_BYTE_ARRAY())
        .name("Parse Kafka Message")
        .filter(filter_spc_weight_data)
        .name("Filter action_type=W, target hwcodes")
    )

    detected = (
        parsed.key_by(lambda r: r.hwcode)
        .process(SpcDetectorFunction(calibrations, config.flink.state_ttl_hours), output_type=Types.PICKLED_BYTE_ARRAY())
        .name("SPC Sustained-Breach Detector")
        .filter(lambda d: d is not None)
        .map(_to_row, output_type=Types.ROW(SPC_READING_FIELD_TYPES))
        .name("To Postgres Row")
    )

    sink_builder = PostgresSinkBuilder(config.postgres)
    sink_builder.build_sink(detected, SPC_READINGS_INSERT_SQL, SPC_READING_FIELD_TYPES)

    return env


def setup_logging(config: AppConfig):
    logging.basicConfig(
        stream=sys.stdout,
        level=getattr(logging, config.log_level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def main():
    config = DEFAULT_CONFIG
    setup_logging(config)

    if not config.postgres.username or not config.postgres.password:
        logging.warning(
            "SPC_PG_USERNAME/SPC_PG_PASSWORD not set -- Postgres sink will fail to "
            "connect until these are provided via environment variables."
        )

    calibrations = fetch_line_calibrations(config.postgres)
    for hwcode, cal in calibrations.items():
        if cal.status != "arl_validated":
            logging.warning(
                "%s is running with status=%s -- alarm/label for this "
                "machine are NOT validated, do not act on them yet.",
                hwcode, cal.status,
            )

    env = build_job(config, calibrations)
    env.execute("SPC Monitoring Job")


if __name__ == "__main__":
    main()
