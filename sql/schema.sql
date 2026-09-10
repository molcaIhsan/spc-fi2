-- SPC schema for ajinomoto_mes (jdbc:postgresql://16.78.150.84:5432/ajinomoto_mes)
--
-- NOT YET APPLIED to the live database -- for review first (need real
-- credentials to apply it; none found in this repo -- see README.md).
-- Matches exactly what spc/flink_job.py produces (SPC_READINGS_INSERT_SQL).
--
-- PoC SCOPE: ANRITSU64-2 (Line 8) only -- the real research machine from
-- alarm-fi2/RESEARCH.md. The 3 production lines explored earlier
-- (ANRITSU54-1/YAMATO-1/YAMATO-2) are deferred: live sampling from FI2.data
-- found their real per-item weight (~1085-1094 raw units) is ~12x the
-- placeholder target_weight=90.0000 previously seeded here, and YAMATO-1/
-- YAMATO-2 were observed running different SKUs simultaneously (sku_code 2
-- vs 10) -- needs reconciling with whoever owns those numbers before it's
-- worth re-adding.
--
-- Two tables:
--   spc_line_config -- one row per machine. The tunable source of truth
--                      (spc/config.py::fetch_line_calibrations reads this at
--                      job-submission time) -- change a parameter here via
--                      UPDATE, no redeploy needed. status='arl_validated' for
--                      ANRITSU64-2 is real, not a placeholder -- it reflects
--                      alarm-fi2/RESEARCH.md's actual Phase I + ARL grid
--                      search on this machine's historical data.
--   spc_readings    -- one row per processed reading. Partitioned by month.

CREATE SCHEMA IF NOT EXISTS ajinomoto_mes;
SET search_path TO ajinomoto_mes;

-- ---------------------------------------------------------------------------
-- 1. spc_line_config -- one row per machine, tunable calibration + status.
-- ---------------------------------------------------------------------------
CREATE TABLE spc_line_config (
    machine_name        TEXT PRIMARY KEY,     -- e.g. 'ANRITSU54-1'
    line                TEXT NOT NULL,
    sku                 TEXT,

    target_weight       NUMERIC(10,4),
    spec_lower          NUMERIC(10,4),         -- absolute value, NOT a +/- offset -- confirmed asymmetric in practice
    spec_upper          NUMERIC(10,4),
    control_lower       NUMERIC(10,4),         -- Phase I 3-sigma, from THIS machine's own baseline, once one exists
    control_upper       NUMERIC(10,4),

    ewma_lambda         NUMERIC(6,4) DEFAULT 0.20,
    kalman_q            NUMERIC(10,6) DEFAULT 0.05,
    kalman_r            NUMERIC(10,6) DEFAULT 4.0,

    sustained_threshold INTEGER DEFAULT 9,    -- k -- tunable: UPDATE this row, no redeploy needed
    sustained_window    INTEGER DEFAULT 50,   -- n -- ditto

    valid_weight_min    NUMERIC(10,4),
    valid_weight_max    NUMERIC(10,4),
    multi_pack_rules    JSONB NOT NULL DEFAULT '[]',  -- [{"min":.., "max":.., "divisor":..}, ...]

    status              TEXT NOT NULL DEFAULT 'uncalibrated_default'
                         CHECK (status IN ('uncalibrated_default', 'phase1_baseline_only', 'arl_validated')),
    status_reason       TEXT,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Real, validated calibration -- alarm-fi2/RESEARCH.md's Phase I 3-sigma +
-- ARL0/ARL1 grid search on ANRITSU64-2's actual historical data (not a
-- placeholder, unlike everything seeded here previously).
INSERT INTO spc_line_config (machine_name, line, target_weight, spec_lower, spec_upper, control_lower, control_upper, sustained_threshold, sustained_window, valid_weight_min, valid_weight_max, multi_pack_rules, status, status_reason) VALUES
    ('ANRITSU64-2', '8', 250.0, 247.0, 253.0, 248.8193900839789, 251.1806099160211, 9, 50, 200.0, 300.0,
     '[{"min":450.0,"max":550.0,"divisor":2.0},{"min":700.0,"max":800.0,"divisor":3.0},{"min":950.0,"max":1050.0,"divisor":4.0}]',
     'arl_validated', 'Phase I baseline + full ARL0/ARL1 grid search, alarm-fi2/RESEARCH.md §4.4')
ON CONFLICT (machine_name) DO NOTHING;

-- ---------------------------------------------------------------------------
-- 2. spc_readings -- one row per processed reading. Partitioned by month.
-- ---------------------------------------------------------------------------
CREATE TYPE spc_label AS ENUM ('LSL', 'LCL', 'Normal', 'UCL', 'USL');

CREATE TABLE spc_readings (
    reading_id      BIGSERIAL,
    machine_name    TEXT NOT NULL,
    line            TEXT NOT NULL,
    sku             TEXT NOT NULL,
    run_id          TEXT NOT NULL,          -- synthesized (SKU-change-triggered), not derived from any external run tracker
    event_timestamp TIMESTAMPTZ NOT NULL,

    raw_data        DOUBLE PRECISION NOT NULL,   -- raw cumulative totalizer value as received
    raw_delta       DOUBLE PRECISION NOT NULL,   -- delta before multi-pack division
    unit_weight     DOUBLE PRECISION NOT NULL,   -- after multi-pack division
    pack_count      SMALLINT NOT NULL,

    ewma            DOUBLE PRECISION,
    kalman          DOUBLE PRECISION,
    is_breach       BOOLEAN NOT NULL,            -- single-point EWMA-or-Kalman breach (noisy, not itself an alarm)
    label           spc_label NOT NULL,
    alarm           BOOLEAN NOT NULL,             -- the sustained-breach alarm (k/n from spc_line_config, tunable -- see RESEARCH.md)

    config_status   TEXT NOT NULL,                -- copied from spc_line_config.status at processing time -- don't trust label/alarm if this isn't 'arl_validated'
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (reading_id, event_timestamp)
) PARTITION BY RANGE (event_timestamp);

CREATE TABLE spc_readings_2026_09 PARTITION OF spc_readings
    FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');
CREATE TABLE spc_readings_2026_10 PARTITION OF spc_readings
    FOR VALUES FROM ('2026-10-01') TO ('2026-11-01');
-- Needs an ongoing process to create future partitions (pg_partman, or a
-- scheduled job) before they're needed -- inserts fail otherwise (verified
-- against a real local Postgres during development).

CREATE INDEX idx_spc_readings_machine_time ON spc_readings (machine_name, event_timestamp);
CREATE INDEX idx_spc_readings_run ON spc_readings (run_id, event_timestamp);
CREATE INDEX idx_spc_readings_alarm ON spc_readings (event_timestamp) WHERE alarm;
