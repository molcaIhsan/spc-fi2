"""
Configuration for the production SPC drift detector.

PROVENANCE: every number here was derived in the research repo `alarm-fi2`,
specifically `RESEARCH.md` (the experiment log) and `history_runs/arl_calibration_no_cusum/`
(the actual grid-search artifacts). This repo does not re-derive anything --
it only runs the already-validated decision logic. If a parameter here ever
needs to change, go recalibrate in `alarm-fi2`, then update it here; don't
hand-tune it in this repo.

STATUS: PROVISIONAL, not final. SUSTAINED_THRESHOLD/SUSTAINED_WINDOW below are
the best candidate found against an illustrative false-alarm budget -- the real
budget has to come from whoever owns the production line (RESEARCH.md Action
Plan #1-2). Expect this file to be revisited once that number exists.
"""

from pathlib import Path


class Config:
    # -------------------------------------------------------------------------
    # Segment identity -- one row today (single hwcode/sku). Add more rows to
    # this structure (not more global constants) when a second machine/SKU
    # shows up; re-derive its own calibration in alarm-fi2 first.
    # -------------------------------------------------------------------------
    HWCODE = "ANRITSU64-2"
    SKU = "250 GR-RC"

    # -------------------------------------------------------------------------
    # Product & sensor specification
    # -------------------------------------------------------------------------
    TARGET_WEIGHT = 250.0
    SPEC_LOWER = 247.0
    SPEC_UPPER = 253.0

    # Multi-pack normalization (alarm-fi2 src/preprocessing.py -- confirmed
    # correct, ~99.92% row survival on the full historical dataset).
    MULTI_PACK_RULES = [
        (450.0, 550.0, 2.0),
        (700.0, 800.0, 3.0),
        (950.0, 1050.0, 4.0),
    ]
    VALID_WEIGHT_MIN = 200.0
    VALID_WEIGHT_MAX = 300.0

    # -------------------------------------------------------------------------
    # Estimators (EWMA + Kalman) -- alarm-fi2 config.py defaults, unchanged.
    # -------------------------------------------------------------------------
    EWMA_LAMBDA = 0.20
    KALMAN_Q = 0.05
    KALMAN_R = 4.0

    # -------------------------------------------------------------------------
    # Control limits -- Phase I 3-sigma calibration on confirmed-clean baseline
    # runs only (alarm-fi2 history_runs/arl_calibration_no_cusum/baseline_calibration.json).
    # sigma_ewma = 0.3935g -> control = target +/- 3*sigma.
    # -------------------------------------------------------------------------
    CONTROL_LOWER = 248.8193900839789
    CONTROL_UPPER = 251.1806099160211

    # CUSUM is calibrated but INTENTIONALLY NOT part of the breach trigger --
    # OR-ing it in (tested once) collapsed the false-alarm budget from an ARL0
    # of 6,324 items to 360 (RESEARCH.md §4.4). Kept here only if a future,
    # properly re-calibrated CUSUM+breach combination is validated -- do not
    # wire it into DriftDetector.is_breach without re-running that grid search.
    CUSUM_K = 0.19676831933684757
    CUSUM_H = 1.5741465546947806

    # -------------------------------------------------------------------------
    # Sustained-breach rule (k-of-n) -- PROVISIONAL, see module docstring.
    # Best candidate found at an illustrative ARL0>=5,000 target:
    #   ARL0 ~5,159 items, miss rate 1.7%, mean detection delay ~419 items.
    # Alternative on file if zero-miss is prioritized over false-alarm rate:
    #   SUSTAINED_THRESHOLD, SUSTAINED_WINDOW = 7, 50
    #   (ARL0 ~3,715, miss rate 0.0%, mean delay ~310 items)
    # -------------------------------------------------------------------------
    SUSTAINED_THRESHOLD = 9
    SUSTAINED_WINDOW = 50

    # State TTL for the streaming job -- a shift/run is ~8 hours; expire idle
    # per-run state well after that so completed runs don't accumulate forever.
    STATE_TTL_HOURS = 24
