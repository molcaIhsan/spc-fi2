-- SPC schema for ajinomoto_mes (jdbc:postgresql://16.78.150.84:5432/ajinomoto_mes)
--
-- NOT YET APPLIED to the live database -- this is a design for review. Run it
-- yourself (or explicitly ask for it to be run) once the open questions in
-- the accompanying comments are resolved, and once whoever owns this schema
-- has approved a new set of tables in it.
--
-- Four tables:
--   spc_line_config  -- versioned, per-(line, hwcode, sku) calibration. This
--                        is the honest record of "is this line even
--                        calibrated yet" -- as of this design, NONE of
--                        ANRITSU54-1 / YAMATO-1 / YAMATO-2 have a real
--                        Phase I baseline; ANRITSU64-2's numbers (RESEARCH.md)
--                        are for a machine that isn't one of these 3 lines
--                        and must not be reused as if they were.
--   spc_readings     -- one row per processed reading. Highest volume table
--                        (a single machine produced ~47k rows/day in the
--                        historical research dataset -- expect ~100k-150k/day
--                        across 3 lines, comfortably within Postgres if
--                        partitioned, not "big data" scale). Partitioned by
--                        month on event_timestamp.
--   spc_drift_events -- one row per alarm (sustained_drift False->True
--                        transition), with resolution tracking. Low volume,
--                        the operationally important table.
--   spc_runs         -- one row per run/shift, a rollup for reporting.

CREATE SCHEMA IF NOT EXISTS ajinomoto_mes;
SET search_path TO ajinomoto_mes;

-- Must exist BEFORE spc_line_config, which uses a gist exclusion constraint
-- over text columns -- btree_gist supplies the operator classes for that.
-- (Caught by actually running this DDL against a real Postgres before
-- treating it as final -- it errors with a cascading failure otherwise.)
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- ---------------------------------------------------------------------------
-- 1. spc_line_config -- versioned calibration per (line, hwcode, sku).
-- ---------------------------------------------------------------------------
CREATE TABLE spc_line_config (
    config_id           BIGSERIAL PRIMARY KEY,
    line                TEXT NOT NULL,              -- e.g. 'line_1', 'line_2', 'line_3'
    hwcode              TEXT NOT NULL,              -- e.g. 'ANRITSU54-1', 'YAMATO-1', 'YAMATO-2'
    sku                 TEXT NOT NULL,

    target_weight       NUMERIC(10,4) NOT NULL,
    spec_lower          NUMERIC(10,4) NOT NULL,     -- absolute value, NOT a +/- offset -- confirmed asymmetric in practice
    spec_upper          NUMERIC(10,4) NOT NULL,

    -- Multi-pack normalization bins for this line -- may differ from
    -- ANRITSU64-2's (450-550/700-800/950-1050) if target_weight differs.
    multi_pack_rules    JSONB NOT NULL DEFAULT '[]',  -- [{"min":.., "max":.., "divisor":..}, ...]
    valid_weight_min    NUMERIC(10,4) NOT NULL,
    valid_weight_max    NUMERIC(10,4) NOT NULL,

    ewma_lambda         NUMERIC(6,4) NOT NULL,
    kalman_q            NUMERIC(10,6) NOT NULL,
    kalman_r            NUMERIC(10,6) NOT NULL,

    control_lower       NUMERIC(10,4) NOT NULL,     -- Phase I 3-sigma, from THIS line's own baseline
    control_upper       NUMERIC(10,4) NOT NULL,
    cusum_k             NUMERIC(10,6),
    cusum_h             NUMERIC(10,6),

    sustained_threshold INTEGER NOT NULL,           -- k
    sustained_window    INTEGER NOT NULL,           -- n

    -- Honesty field: don't let a row default to looking "validated" when it
    -- isn't. See status_reason for what backs this classification.
    status              TEXT NOT NULL DEFAULT 'uncalibrated_default'
                         CHECK (status IN ('uncalibrated_default', 'phase1_baseline_only', 'arl_validated')),
    status_reason        TEXT,                       -- e.g. "no baseline data collected yet" / link to the grid search run

    effective_from       TIMESTAMPTZ NOT NULL DEFAULT now(),
    effective_to         TIMESTAMPTZ,                -- NULL = currently active
    created_by            TEXT,
    notes                 TEXT,

    CONSTRAINT spc_line_config_no_overlap
        EXCLUDE USING gist (
            line WITH =, hwcode WITH =, sku WITH =,
            tstzrange(effective_from, COALESCE(effective_to, 'infinity')) WITH &&
        )
);

CREATE INDEX idx_spc_line_config_active
    ON spc_line_config (line, hwcode, sku)
    WHERE effective_to IS NULL;

-- ---------------------------------------------------------------------------
-- 2. spc_readings -- one row per processed reading. Partitioned by month.
-- ---------------------------------------------------------------------------
CREATE TABLE spc_readings (
    reading_id      BIGSERIAL,
    line            TEXT NOT NULL,
    hwcode          TEXT NOT NULL,
    sku             TEXT NOT NULL,
    run_id          TEXT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,

    raw_weight      NUMERIC(14,4),      -- cumulative totalizer reading, if worth keeping for audit
    unit_weight     NUMERIC(10,4) NOT NULL,
    pack_count      SMALLINT NOT NULL,

    ewma            NUMERIC(10,4),
    kalman          NUMERIC(10,4),
    is_breach       BOOLEAN NOT NULL,
    is_reject       BOOLEAN NOT NULL,   -- unit_weight outside [spec_lower, spec_upper] -- separate from is_breach on purpose (control limit != spec limit, see RESEARCH.md)
    sustained_drift BOOLEAN NOT NULL,

    config_id       BIGINT NOT NULL REFERENCES spc_line_config (config_id),
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (reading_id, event_timestamp)  -- partition key must be in the PK
) PARTITION BY RANGE (event_timestamp);

-- Example partitions -- this needs an ongoing process to create future
-- partitions (pg_partman, or a scheduled job) before they're needed. Not set
-- up here; flagging it as a real operational requirement, not an afterthought.
CREATE TABLE spc_readings_2026_09 PARTITION OF spc_readings
    FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');
CREATE TABLE spc_readings_2026_10 PARTITION OF spc_readings
    FOR VALUES FROM ('2026-10-01') TO ('2026-11-01');

CREATE INDEX idx_spc_readings_line_time ON spc_readings (line, hwcode, sku, event_timestamp);
CREATE INDEX idx_spc_readings_run ON spc_readings (run_id, event_timestamp);
CREATE INDEX idx_spc_readings_breach ON spc_readings (event_timestamp) WHERE is_breach;

-- ---------------------------------------------------------------------------
-- 3. spc_drift_events -- one row per alarm (onset event), with resolution.
-- ---------------------------------------------------------------------------
CREATE TABLE spc_drift_events (
    event_id            BIGSERIAL PRIMARY KEY,
    line                TEXT NOT NULL,
    hwcode              TEXT NOT NULL,
    sku                 TEXT NOT NULL,
    run_id              TEXT NOT NULL,

    onset_timestamp     TIMESTAMPTZ NOT NULL,
    onset_ewma          NUMERIC(10,4),
    onset_kalman        NUMERIC(10,4),
    breach_count_in_window INTEGER,   -- how many of the last n readings were breaches at onset

    resolved_timestamp  TIMESTAMPTZ,   -- when sustained_drift went back to False; NULL = still open
    config_id           BIGINT NOT NULL REFERENCES spc_line_config (config_id),

    status              TEXT NOT NULL DEFAULT 'open'
                         CHECK (status IN ('open', 'acknowledged', 'resolved')),
    acknowledged_by     TEXT,
    acknowledged_at     TIMESTAMPTZ,
    notes               TEXT
);

CREATE INDEX idx_spc_drift_events_open ON spc_drift_events (line, hwcode, sku) WHERE status != 'resolved';
CREATE INDEX idx_spc_drift_events_time ON spc_drift_events (onset_timestamp);

-- ---------------------------------------------------------------------------
-- 4. spc_runs -- per-run/shift rollup, for reporting.
-- ---------------------------------------------------------------------------
CREATE TABLE spc_runs (
    run_id              TEXT PRIMARY KEY,
    line                TEXT NOT NULL,
    hwcode              TEXT NOT NULL,
    sku                 TEXT NOT NULL,
    start_time          TIMESTAMPTZ NOT NULL,
    end_time            TIMESTAMPTZ,

    total_items         INTEGER NOT NULL DEFAULT 0,
    total_breaches      INTEGER NOT NULL DEFAULT 0,
    total_rejects       INTEGER NOT NULL DEFAULT 0,
    total_onset_events  INTEGER NOT NULL DEFAULT 0,

    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
