# spc-fi2

Lean, production-track SPC drift detector for the `ANRITSU64-2` checkweigher
(`250 GR-RC` SKU). Runs the validated sustained-breach k-of-n detection logic
as a PyFlink streaming job.

**This repo intentionally contains only the runtime detector** — multi-pack
normalization, the EWMA+Kalman breach detector, and the Flink job. No ML
classifiers, no ARL-calibration/research tooling, and no hopper feedback
controller (EPC). All of that stays in the research repo, `alarm-fi2`, where
every number here was derived and can be re-derived.

## Provenance

Every constant in `spc/config.py` traces back to `alarm-fi2/RESEARCH.md` (the
experiment log) and `alarm-fi2/history_runs/arl_calibration_no_cusum/` (the
actual grid-search artifacts). If a parameter needs to change, recalibrate it
there first — don't hand-tune it here.

## Status: provisional, not final

- **`(k, n) = (9, 50)`** is the best candidate found against an *illustrative*
  false-alarm budget, not a real one supplied by whoever owns the production
  line. `RESEARCH.md`'s Action Plan items #1-2 are still open. Expect this to
  change once that number exists — the alternative already on file if
  zero-miss is prioritized over false-alarm rate is `(k, n) = (7, 50)` (see
  the comment in `config.py`).
- **Estimator is EWMA+Kalman**, not raw `unit_weight` — raw won an ablation in
  `alarm-fi2` but only on a reduced grid (Action Plan #4 not yet run). Ship
  the fully-validated combination; don't switch to raw until that grid exists.
- **A real bug was caught and fixed while porting this**: an earlier version of
  the detector (in `alarm-fi2/src/filters.py`) OR'd CUSUM into the breach
  trigger. The actual grid search proved that collapses the false-alarm budget
  (ARL0 6,324 → 360). This repo's `detector.py` computes CUSUM for visibility
  but excludes it from `is_breach` — see the regression test in
  `spc/tests/test_detector.py::test_cusum_exceeding_its_threshold_does_not_trigger_is_breach`
  that guards against this regressing.

## Layout

```
spc/
  config.py        # all calibrated constants, provenance comments
  preprocessing.py  # streaming multi-pack normalization
  detector.py       # the sustained-breach k-of-n detector
  flink_job.py       # PyFlink job: Kafka -> normalize -> detect -> Kafka
  tests/             # pytest -- run before every change
```

## Running tests

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
python -m pytest spc/tests/ -v
```

## Docker

```bash
docker build -t spc-detector:latest .
```

See the comment block at the bottom of `Dockerfile` for how to submit the job
to a running Flink cluster from this image — you'll need to fill in your
JobManager address and Kafka topic names.

## Before deploying to a real cluster

1. **Verify the PyFlink API surface against your actual Flink cluster's
   version.** This targets the DataStream API as of Flink 1.18 — state
   descriptor and Kafka connector builder APIs have moved across minor
   versions. Don't trust the imports in `flink_job.py` blind; check
   `pyflink.__version__` against your cluster.
2. **Fill in real topic names** — `KAFKA_BOOTSTRAP_SERVERS`, `SOURCE_TOPIC`,
   `SINK_TOPIC` in `flink_job.py` are environment-variable-overridable
   placeholders (`CHANGE_ME:9092` etc.), built against the raw schema
   `{date_bucket, hwcode, timestamp, id, run_id, sku, weight}` from
   `alarm-fi2/dataset/weight_anritsu64-2.csv`. Confirm your real topic's
   schema matches before wiring this up.
3. **Get the real false-alarm budget** from whoever owns the production line,
   then confirm or change `(k, n)` in `config.py` accordingly.
4. **CI currently only builds the Docker image, it doesn't push anywhere** —
   `.github/workflows/ci.yml` has a comment marking where to add a registry
   push step once you have a container registry and production target picked.

## What's explicitly not here yet

- The hopper feedback controller (EPC) — its gain (`g`) is uncalibrated, so it
  isn't safe to wire to a real actuator. See `alarm-fi2/src/controller.py` and
  `RESEARCH.md`'s Action Plan for the step test that has to happen first.
- A raw-`unit_weight` detector variant — the ablation that found it promising
  only ran a reduced grid; don't add it here until `alarm-fi2`'s Action Plan
  #4 validates it across the full grid.
