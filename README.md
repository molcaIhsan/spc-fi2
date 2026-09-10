# spc-fi2

Standalone SPC drift detector for 3 checkweighers: `ANRITSU54-1` (Line 1),
`YAMATO-1` (Line 2), `YAMATO-2` (Line 3). Runs the validated sustained-breach
k-of-n detection logic as a PyFlink streaming job, reading `FI2.data` from
Kafka directly (own consumer group, independent of any other job on the
cluster) and writing to Postgres (`ajinomoto_mes` schema).

**Independent repo, no dependency on ajinomoto-etl-flink** — an earlier
iteration of this job lived as `src/spc/` inside that repo, reusing its
`fi2` package for message parsing and machine-id mappings; that approach was
dropped in favor of this fully standalone one (still present on that repo's
`add-spc-monitoring` branch if ever needed again, just not being developed
further).

## Provenance

Every parameter in `spc/config.py`/`sql/schema.sql` traces back to a separate
research repo, `alarm-fi2` (`RESEARCH.md`, the experiment log, and
`history_runs/arl_calibration_no_cusum/`) — but that research was done
entirely on `ANRITSU64-2` (Line 8), **a different machine** from the three
this job targets. Confirmed directly against the real machine registry in
ajinomoto-etl-flink (`fi2/models/enums.py`'s `MachineId` enum) before this
was built. None of the three target lines have their own Phase I baseline
yet — see Status below.

## Status: provisional, not final

- **`(k, n) = (9, 40)`** on all 3 machines — the best k=9 candidate found for
  the *research* machine at window sizes 40 and 50 (near-identical
  performance), not independently validated for these 3. `RESEARCH.md`'s
  Action Plan items #1-2 (get a real false-alarm budget) are still open.
- **`YAMATO-1` and `YAMATO-2` share `ANRITSU54-1`'s numbers** (target
  90.0000, spec 87.0030/93.9960) — per instruction, but **assumed**, not
  independently confirmed for either Yamato line specifically.
- **Config is tunable via Postgres, not hardcoded**: `spc/config.py::fetch_line_calibrations()`
  loads from `ajinomoto_mes.spc_line_config` at job-submission time — change
  a parameter with `UPDATE`, no redeploy. Falls back (loudly) to hardcoded
  defaults if Postgres isn't reachable.
- **A real bug was caught and fixed while building this**: an earlier
  version of the detector OR'd CUSUM into the breach trigger, which the
  actual grid search (on the research machine) proved collapses the
  false-alarm budget (ARL0 6,324 → 360). Excluded here, with a regression
  test guarding against it recurring (`spc/tests/test_detector.py`).

## Layout

```
spc/
  config.py          # calibration (LineCalibration), fetch_line_calibrations() from Postgres
  models.py           # Label enum (LSL/LCL/Normal/UCL/USL) + classify()
  message_parser.py    # raw Kafka message format: @#HWCODE#ACTION#QTY#QUALITY#TIMESTAMP#SKU_CODE#REASON#!
  preprocessing.py      # streaming multi-pack normalization
  detector.py            # sustained-breach k-of-n detector
  flink_job.py            # Kafka -> filter -> detect -> Postgres
  sinks/postgres_sink.py   # JDBC sink builder
  tests/                    # pytest -- run before every change
sql/schema.sql               # spc_line_config (tunable) + spc_readings (partitioned)
```

## Running tests

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-test.txt
python -m pytest spc/tests/ -v
```

22 tests, all passing: multi-pack normalization, detector label/alarm
transitions (including the CUSUM-exclusion regression test), message parsing
against a real captured sample, and the hwcode→line mapping.

## Docker

```bash
docker build -t spc-detector:latest .
```

Base image `flink:1.20.1` — confirmed as the real target cluster's version
(checked directly against ajinomoto-etl-flink's own Dockerfile). See the
comment block at the bottom of `Dockerfile` for how to submit the job to a
running cluster.

## Before deploying to a real cluster

1. **Verify the PyFlink API surface against your actual Flink cluster's
   version.** Targets the DataStream API as of Flink 1.20 — don't trust the
   imports in `flink_job.py` blind.
2. **JDBC connector + Postgres driver + Kafka connector JAR versions in
   `Dockerfile` are unverified** against Maven Central (no network access
   while writing this).
3. **Confirm the Kafka broker addresses** — `12.234.248.167:9092`,
   `10.234.226.41:9092` were given directly for this job, but at least two
   other different addresses exist across the broader ajinomoto-etl-flink
   repo (a stale default, an ENV override) — this repo's own history of
   inconsistency means none of these should be trusted without confirming.
4. **Postgres credentials** — `SPC_PG_USERNAME`/`SPC_PG_PASSWORD` env vars,
   never hardcoded. No `.env`/secrets file exists in ajinomoto-etl-flink's
   checkout either (confirmed) — these have to come from you directly.
5. **`sql/schema.sql` not yet applied** to the real database — need
   credentials first, then review before applying.
6. **Confirm `YAMATO-1`/`YAMATO-2` really do share `ANRITSU54-1`'s
   target/spec numbers** — currently an assumption from instruction, not
   independently verified per line.
7. **Real Phase I baseline + ARL grid search per line** — not done. This job
   can run in "collect data, don't act on the alarms yet" mode today (that's
   what `config_status` on every row is for).

## What's explicitly not here

- The hopper feedback controller (EPC) — needs a real gain (`g`)
  measurement first, not built for any of these 3 machines.
- A `raw`-estimator detector variant — an ablation on the research machine
  found it promising, but only on a reduced grid; not validated for these
  machines either.
