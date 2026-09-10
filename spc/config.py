"""
SPC job configuration -- standalone, no dependency on ajinomoto-etl-flink/fi2.

STATUS: 3 target machines, none with a real Phase I baseline (see
LINE_CALIBRATIONS below). Config is tunable via Postgres (spc_line_config,
sql/schema.sql) -- fetch_line_calibrations() loads it at job-submission time,
so changing a parameter is an UPDATE statement, not a redeploy. Falls back
(loudly) to the hardcoded defaults below if Postgres isn't reachable.
"""

import os
from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class KafkaConfig:
    # Three different bootstrap-server values have shown up across the
    # broader ajinomoto-etl-flink repo (stale defaults, an ENV override) --
    # these two were given directly for this job. CONFIRM before deploying.
    bootstrap_server: str = os.getenv(
        "SPC_KAFKA_BOOTSTRAP_SERVERS", "12.234.248.167:9092,10.234.226.41:9092"
    )
    topic: str = "FI2.data"
    group_id: str = os.getenv("SPC_KAFKA_GROUP_ID", "spc_monitoring")


@dataclass
class PostgresConfig:
    """jdbc:postgresql://16.78.150.84:5432/ajinomoto_mes, schema ajinomoto_mes.
    No credentials given yet -- must come from env, never hardcoded/committed."""

    host: str = os.getenv("SPC_PG_HOST", "16.78.150.84")
    port: int = int(os.getenv("SPC_PG_PORT", "5432"))
    database: str = os.getenv("SPC_PG_DATABASE", "ajinomoto_mes")
    schema: str = os.getenv("SPC_PG_SCHEMA", "ajinomoto_mes")
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
    """One machine's SPC parameters. STATUS TRACKS WHETHER THIS IS REAL.

    None of the three target lines have a Phase I baseline yet -- confirmed
    directly against the ajinomoto-etl-flink repo's MachineId enum:
    ANRITSU64-2 (the only machine with real calibration, in the separate
    alarm-fi2/RESEARCH.md research) is FI2_8_C22 on Line 8, not one of these
    three. Every value below is a placeholder until real baseline data is
    collected and calibrated per machine.
    """

    target_weight: float
    spec_lower: float
    spec_upper: float
    control_lower: float
    control_upper: float
    ewma_lambda: float = 0.20
    kalman_q: float = 0.05
    kalman_r: float = 4.0
    sustained_threshold: int = 9   # k -- provisional, tunable via spc_line_config
    sustained_window: int = 40     # n -- provisional (9-of-40 was a validated grid candidate in the
                                    # separate alarm-fi2 research: ARL0~5257, miss 1.7%, delay ~417-419 items --
                                    # for ANRITSU64-2, NOT for these 3 machines)
    valid_weight_min: float = 0.0
    valid_weight_max: float = 0.0
    multi_pack_rules: tuple = ()   # ((min, max, divisor), ...)
    status: str = "uncalibrated_default"


# hwcode -> line, self-contained (previously derived from ajinomoto-etl-flink's
# MachineId enum; hardcoded here since this repo no longer imports that repo).
# Re-verify against that enum if it ever changes.
_HWCODE_TO_LINE = {
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
# YAMATO-1/YAMATO-2 share ANRITSU54-1's numbers here, per instruction
# ("yamato 1 and 2 use the same config for weight") -- ASSUMED, not
# independently confirmed for either Yamato line specifically.
_ANRITSU54_1_FALLBACK = LineCalibration(
    target_weight=90.0000,
    spec_lower=87.0030,
    spec_upper=93.9960,
    control_lower=88.5,   # PLACEHOLDER -- not a Phase I calibration, just inside spec as a stopgap
    control_upper=91.5,   # PLACEHOLDER
    valid_weight_min=60.0,
    valid_weight_max=120.0,
    status="uncalibrated_default",
)
LINE_CALIBRATIONS: Dict[str, LineCalibration] = {
    "ANRITSU54-1": _ANRITSU54_1_FALLBACK,
    "YAMATO-1": _ANRITSU54_1_FALLBACK,
    "YAMATO-2": _ANRITSU54_1_FALLBACK,
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
