"""
Lightweight standalone PoC consumer -- no Flink, no Docker. Reuses the exact
same pure-Python detector/preprocessing/parser/models modules already tested
in spc/tests/, just driven by a plain kafka-python consumer instead of
PyFlink's DataStream API. Good enough for a single-machine PoC on one
hwcode (ANRITSU64-2); reintroduce Flink if/when this needs to scale to
multiple machines with fault-tolerant distributed state.

Run: python poc_consumer.py
Needs: pip install kafka-python psycopg2-binary
Env:  SPC_PG_USERNAME / SPC_PG_PASSWORD (required, never hardcoded)
"""

import logging
import sys

import psycopg2
from kafka import KafkaConsumer

from spc.config import DEFAULT_CONFIG, TARGET_HWCODES, fetch_line_calibrations, get_line_for_hwcode
from spc.message_parser import parse_message
from spc.preprocessing import MultiPackNormalizer
from spc.detector import DriftDetector

logging.basicConfig(
    stream=sys.stdout, level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def _build_insert_sql(schema: str) -> str:
    # Schema name has to come from config, not be hardcoded here -- was
    # literally "ajinomoto_mes" regardless of SPC_PG_SCHEMA until this fix,
    # meaning writes silently ignored any override, unlike the calibration
    # read path (fetch_line_calibrations), which already used it correctly.
    return f"""
        INSERT INTO {schema}.spc_readings
        (machine_name, line, sku, run_id, event_timestamp, raw_data, raw_delta,
         unit_weight, pack_count, ewma, kalman, is_breach, label, alarm, config_status)
        VALUES (%s, %s, %s, %s, to_timestamp(%s), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """


def main():
    config = DEFAULT_CONFIG
    insert_sql = _build_insert_sql(config.postgres.schema)

    if not config.postgres.username or not config.postgres.password:
        logging.error(
            "SPC_PG_USERNAME/SPC_PG_PASSWORD not set -- refusing to start "
            "without them (never hardcode credentials)."
        )
        sys.exit(1)

    calibrations = fetch_line_calibrations(config.postgres)
    for hwcode, cal in calibrations.items():
        if cal.status != "arl_validated":
            logging.warning(
                "%s running with status=%s -- label/alarm NOT validated, "
                "don't act on them yet.", hwcode, cal.status,
            )

    pg_conn = psycopg2.connect(
        host=config.postgres.host, port=config.postgres.port,
        dbname=config.postgres.database,
        user=config.postgres.username, password=config.postgres.password,
    )
    pg_conn.autocommit = True
    logging.info("Connected to Postgres at %s:%s/%s", config.postgres.host, config.postgres.port, config.postgres.database)

    consumer = KafkaConsumer(
        config.kafka.topic,
        bootstrap_servers=config.kafka.bootstrap_server.split(","),
        group_id=config.kafka.group_id,
        auto_offset_reset="latest",   # PoC: only new messages from now, not the full backlog
        enable_auto_commit=True,
    )
    logging.info("Consuming %s from %s (group_id=%s), filtering for %s",
                 config.kafka.topic, config.kafka.bootstrap_server, config.kafka.group_id, sorted(TARGET_HWCODES))

    # hwcode -> (sku_code, run_id, MultiPackNormalizer, DriftDetector)
    state: dict = {}

    for msg in consumer:
        try:
            record = parse_message(msg.value.decode("utf-8", errors="replace"))
        except Exception:
            logging.exception("Failed to decode message, skipping")
            continue

        if record is None or record.action_type != "W" or record.hwcode not in TARGET_HWCODES:
            continue

        calibration = calibrations[record.hwcode]
        prev = state.get(record.hwcode)
        is_new_run = prev is None or prev[0] != record.sku_code
        if is_new_run:
            run_id = f"{record.hwcode}_{record.sku_code}_{int(record.timestamp)}"
            normalizer = MultiPackNormalizer(calibration)
            detector = DriftDetector(calibration)
        else:
            _, run_id, normalizer, detector = prev

        norm_result = normalizer.update(record.value)
        state[record.hwcode] = (record.sku_code, run_id, normalizer, detector)

        if norm_result is None:
            continue  # first reading of this run, or an implausible delta -- discarded
        raw_delta, unit_weight, pack_count = norm_result

        result = detector.update(unit_weight)

        with pg_conn.cursor() as cur:
            cur.execute(insert_sql, (
                record.hwcode, get_line_for_hwcode(record.hwcode), record.sku_code, run_id,
                record.timestamp / 1000.0, record.value, raw_delta, unit_weight, pack_count,
                result["ewma"], result["kalman"], result["is_breach"], result["label"],
                result["sustained_drift"], calibration.status,
            ))

        logging.info(
            "%s unit_weight=%.2f ewma=%.2f label=%s alarm=%s",
            record.hwcode, unit_weight, result["ewma"], result["label"], result["sustained_drift"],
        )


if __name__ == "__main__":
    main()
