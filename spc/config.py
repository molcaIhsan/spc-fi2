"""
SPC job configuration -- standalone, no dependency on ajinomoto-etl-flink/fi2.

STATUS: PoC scope is ANRITSU64-2 only (Line 8, FI2_8_C22) -- the actual
research machine every number in the separate alarm-fi2/RESEARCH.md was
validated on. The 3 real production lines (ANRITSU54-1/YAMATO-1/YAMATO-2)
are deliberately deferred for now: live sampling from FI2.data found their
real per-item weight (~1085-1094 raw units) is ~12x the placeholder
target_weight=90.0000 we'd been using, and YAMATO-1/YAMATO-2 were observed
running different SKUs at the same time (sku_code 2 vs 10) -- config needs
to be reconciled per (hwcode, sku) with whoever owns those numbers before
that's worth building further. ANRITSU64-2 has no such open question, so
it's the PoC target.

Config is tunable via Postgres (spc_line_config, sql/schema.sql) --
fetch_line_calibrations() loads it at job-submission time, so changing a
parameter is an UPDATE statement, not a redeploy. Falls back (loudly) to
the hardcoded defaults below if Postgres isn't reachable.
"""

import os
from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class KafkaConfig:
    # Confirmed directly (kafkacat -L metadata, and cross-checked against a
    # Kafka broker-overview dashboard): broker 1 is 10.234.248.167 -- the
    # "12.234.248.167" given earlier was a typo (12 vs 10 in the first
    # octet), which is why it looked unreachable. Both brokers are Kafka
    # 4.3.0 (KRaft). FI2.data's only partition is led by broker 2
    # (10.234.226.41) -- broker 1 isn't actually required to consume this
    # specific topic, but both are listed since a client should still know
    # the full cluster for metadata/failover.
    bootstrap_server: str = os.getenv(
        "SPC_KAFKA_BOOTSTRAP_SERVERS", "10.234.248.167:9092,10.234.226.41:9092"
    )
    topic: str = "FI2.data"
    group_id: str = os.getenv("SPC_KAFKA_GROUP_ID", "spc_monitoring")


@dataclass
class PostgresConfig:
    """jdbc:postgresql://16.78.150.84:5432/ajinomoto_mes, schema 'spc'.
    Database and schema are two different things that happened to share the
    name "ajinomoto_mes" earlier -- the database really is named that, but
    the schema our tables actually live in (confirmed by the user, and by
    apply_schema.py's real run) is 'spc', not 'ajinomoto_mes'.
    No credentials given yet -- must come from env, never hardcoded/committed."""

    host: str = os.getenv("SPC_PG_HOST", "16.78.150.84")
    port: int = int(os.getenv("SPC_PG_PORT", "5432"))
    database: str = os.getenv("SPC_PG_DATABASE", "ajinomoto_mes")
    schema: str = os.getenv("SPC_PG_SCHEMA", "spc")
    username: str = os.getenv("SPC_PG_USERNAME", "")
    password: str = os.getenv("SPC_PG_PASSWORD", "")

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"


@dataclass
class FlinkConfig:
    checkpoint_interval: int = int(os.getenv("CHECKPOINT_INTERVAL", "1000"))
    state_ttl_hours: int = int(os.getenv("SPC_STATE_TTL_HOURS", "24"))


@dataclass
class LineCalibration:
    """One machine's SPC parameters. STATUS TRACKS WHETHER THIS IS REAL."""

    target_weight: float
    spec_lower: float
    spec_upper: float
    control_lower: float
    control_upper: float
    ewma_lambda: float = 0.20
    kalman_q: float = 0.05
    kalman_r: float = 4.0
    sustained_threshold: int = 9   # k -- RESEARCH.md's primary recommendation for ANRITSU64-2
    sustained_window: int = 50     # n -- ditto (ARL0~5,159, miss 1.7%, delay ~416-419 items;
                                    # 9-of-40 is a near-identical alternative also validated there)
    valid_weight_min: float = 0.0
    valid_weight_max: float = 0.0
    multi_pack_rules: tuple = ()   # ((min, max, divisor), ...)
    status: str = "uncalibrated_default"


# hwcode -> line, self-contained (previously derived from ajinomoto-etl-flink's
# MachineId enum; hardcoded here since this repo no longer imports that repo).
# Re-verify against that enum if it ever changes.
# ANRITSU64-2 = FI2_8_C22 -> Line 8. The 3 production lines explored earlier
# (ANRITSU54-1/YAMATO-1/YAMATO-2 -> Lines 1/2/3) are deferred -- see module
# docstring -- but kept here since the mapping itself is still true.
_HWCODE_TO_LINE = {
    "ANRITSU64-2": "8",
    "ANRITSU54-1": "1",
    "YAMATO-1": "2",
    "YAMATO-2": "3",
}


def get_line_for_hwcode(hwcode: str) -> Optional[str]:
    return _HWCODE_TO_LINE.get(hwcode)


# FALLBACK ONLY -- used when Postgres isn't reachable (e.g. local testing
# without a DB). The real, tunable source of truth is the spc_line_config
# table (sql/schema.sql) -- change a parameter there via UPDATE, no redeploy.
#
# These ARE the real, validated numbers from alarm-fi2/RESEARCH.md's Phase I
# calibration + ARL grid search on ANRITSU64-2's actual historical data --
# not a placeholder, unlike the 3 production lines this repo targeted earlier.
_ANRITSU64_2_CALIBRATION = LineCalibration(
    target_weight=250.0,
    spec_lower=247.0,
    spec_upper=253.0,
    control_lower=248.8193900839789,   # Phase I 3-sigma, confirmed-clean baseline runs only
    control_upper=251.1806099160211,
    valid_weight_min=200.0,
    valid_weight_max=300.0,
    multi_pack_rules=((450.0, 550.0, 2.0), (700.0, 800.0, 3.0), (950.0, 1050.0, 4.0)),
    status="arl_validated",
)
LINE_CALIBRATIONS: Dict[str, LineCalibration] = {
    "ANRITSU64-2": _ANRITSU64_2_CALIBRATION,
}

TARGET_HWCODES = frozenset(LINE_CALIBRATIONS.keys())


def fetch_line_calibrations(pg_config: "PostgresConfig") -> Dict[str, LineCalibration]:
    """Load calibration from ajinomoto_mes.spc_line_config. Falls back to the
    hardcoded LINE_CALIBRATIONS above (with a loud warning) if Postgres isn't
    reachable, so it's always obvious which one was actually in effect."""
    import logging

    try:
        import psycopg2

        conn = psycopg2.connect(
            host=pg_config.host, port=pg_config.port, dbname=pg_config.database,
            user=pg_config.username, password=pg_config.password,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT machine_name, target_weight, spec_lower, spec_upper,
                           control_lower, control_upper, ewma_lambda, kalman_q, kalman_r,
                           sustained_threshold, sustained_window,
                           valid_weight_min, valid_weight_max, multi_pack_rules, status
                    FROM {pg_config.schema}.spc_line_config
                    WHERE machine_name = ANY(%s)
                    """,
                    (list(TARGET_HWCODES),),
                )
                rows = cur.fetchall()
        finally:
            conn.close()

        result = {}
        for row in rows:
            (machine_name, target_weight, spec_lower, spec_upper, control_lower, control_upper,
             ewma_lambda, kalman_q, kalman_r, sustained_threshold, sustained_window,
             valid_weight_min, valid_weight_max, multi_pack_rules, status) = row
            result[machine_name] = LineCalibration(
                target_weight=float(target_weight), spec_lower=float(spec_lower), spec_upper=float(spec_upper),
                control_lower=float(control_lower), control_upper=float(control_upper),
                ewma_lambda=float(ewma_lambda), kalman_q=float(kalman_q), kalman_r=float(kalman_r),
                sustained_threshold=int(sustained_threshold), sustained_window=int(sustained_window),
                valid_weight_min=float(valid_weight_min), valid_weight_max=float(valid_weight_max),
                multi_pack_rules=tuple(
                    (r["min"], r["max"], r["divisor"]) for r in (multi_pack_rules or [])
                ),
                status=status,
            )

        missing = TARGET_HWCODES - result.keys()
        if missing:
            logging.warning(
                "spc_line_config has no row for %s -- falling back to hardcoded "
                "LINE_CALIBRATIONS for those, NOT the tunable DB config.", missing,
            )
            for hwcode in missing:
                result[hwcode] = LINE_CALIBRATIONS[hwcode]
        return result

    except Exception:
        logging.exception(
            "Could not load calibration from Postgres (spc_line_config) -- "
            "falling back to hardcoded LINE_CALIBRATIONS for ALL machines. "
            "Tuning via UPDATE will have NO effect until this is fixed."
        )
        return dict(LINE_CALIBRATIONS)


@dataclass
class AppConfig:
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    postgres: PostgresConfig = field(default_factory=PostgresConfig)
    flink: FlinkConfig = field(default_factory=FlinkConfig)
    log_level: str = os.getenv("LOG_LEVEL", "INFO")


DEFAULT_CONFIG = AppConfig()
