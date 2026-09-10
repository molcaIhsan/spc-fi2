# spc-fi2

SPC drift detector, PoC scope: **`ANRITSU64-2`** (Line 8) — the actual
machine every number in the separate `alarm-fi2`/`RESEARCH.md` research was
validated on. Runs the validated sustained-breach k-of-n detection logic as
a PyFlink streaming job, reading `FI2.data` from Kafka directly (own
consumer group, independent of any other job on the cluster) and writing to
Postgres (`ajinomoto_mes` schema).

**Independent repo, no dependency on ajinomoto-etl-flink** — an earlier
iteration of this job lived as `src/spc/` inside that repo, reusing its
`fi2` package; dropped in favor of this fully standalone one (still present
on that repo's `add-spc-monitoring` branch if ever needed again).

## Why ANRITSU64-2, not the 3 real production lines

This job originally targeted `ANRITSU54-1`/`YAMATO-1`/`YAMATO-2` (Lines
1/2/3). Live sampling from `FI2.data` (via a real Kafka consumer, see
"Kafka access" below) found:
- Their real per-item weight is **~1085-1094 raw units** — about **12x**
  the placeholder `target_weight=90.0000` this repo had been seeded with.
  Confirmed by computing consecutive deltas on real `W` (weight total)
  messages, including seeing clean 2x/3x multi-pack clusters around that
  same base unit.
- `YAMATO-1` and `YAMATO-2` were observed running **different SKUs at the
  same time** (`sku_code` 2 vs 10) — "same config for both" doesn't hold in
  general; calibration needs to key on `(hwcode, sku)`, not just `hwcode`.
- `ANRITSU54-1` never appeared at all in a 5,000-message sample.

None of that needed resolving for `ANRITSU64-2` — it already has a real,
validated Phase I baseline + ARL grid search. So the PoC targets it instead,
deferring the 3 production lines until their numbers are reconciled with
whoever owns them. The hwcode→line mapping still knows about all 4 machines
(`spc/config.py::get_line_for_hwcode`) — nothing was deleted, just deferred.

## Status: mostly real now, one thing still open

- **`(k, n) = (9, 50)`, control limits, spec limits — all real**, from
  `alarm-fi2/RESEARCH.md`'s actual Phase I calibration + ARL0/ARL1 grid
  search on `ANRITSU64-2`'s historical data. `spc_line_config.status =
  'arl_validated'` for this machine reflects that honestly.
- **Config is tunable via Postgres, not hardcoded**: `fetch_line_calibrations()`
  loads from `ajinomoto_mes.spc_line_config` at job-submission time — change
  a parameter with `UPDATE`, no redeploy. Falls back (loudly) to hardcoded
  defaults if Postgres isn't reachable.
- **A real bug was caught and fixed while building this**: an earlier
  version of the detector OR'd CUSUM into the breach trigger, which the
  actual grid search proved collapses the false-alarm budget (ARL0 6,324 →
  360). Excluded here, with a regression test guarding against it recurring.
- **Still open**: this job hasn't been run against the real Kafka topic yet
  (only manually sampled via a console consumer) — see next section.

## Kafka access — what we learned getting here

The brokers (`10.234.248.167:9092`, `10.234.226.41:9092`, Kafka 4.3.0,
KRaft) sit on an internal network not reachable from an arbitrary outside
machine. What actually worked:
- `10.234.248.167` was originally given as `12.234.248.167` — a typo (`12`
  vs `10` in the first octet), confirmed via `kafkacat -L` metadata and
  cross-checked against a Kafka broker-overview dashboard.
- `kafkacat`/librdkafka (a C client) hit a `Required feature not supported
  by broker` error against this broker — almost certainly a librdkafka/Kafka
  4.x compatibility gap, not a real broker problem. The **Java-based**
  `kafka-console-consumer` (same client family PyFlink's `KafkaSource`
  uses) connects cleanly.
- Access requires being on the internal network — remote desktop (RustDesk)
  into a machine already on it worked; a local dev machine needs either a
  VPN profile for that segment, an SSH tunnel through a bastion on that
  network, or (if available) RustDesk's own TCP-tunneling mode forwarding a
  local port to `10.234.248.167:9092`/`10.234.226.41:9092`.
- Real message format confirmed: `@#HWCODE#ACTION#QTY#QUALITY#TIMESTAMP#SKU_CODE#REASON#!`.
  Each machine emits a burst of 4 messages per tick (`A`=Finish Good count,
  `D`=Actual Production count, `G`=Weight Finish Good, `W`=Weight Total —
  the one this job filters for), sharing one timestamp/sku_code.

## Layout

```
spc/
  config.py          # calibration (LineCalibration), fetch_line_calibrations() from Postgres
  models.py           # Label enum (LSL/LCL/Normal/UCL/USL) + classify() + hwcode->line
  message_parser.py    # raw Kafka message format
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

23 tests, all passing.

## Docker

```bash
docker build -t spc-detector:latest .
```

Base image `flink:1.20.1` — confirmed as the real target cluster's version
(checked directly against ajinomoto-etl-flink's own Dockerfile).

## Before deploying to a real cluster

1. **Verify the PyFlink API surface against your actual Flink cluster's
   version** — don't trust the imports in `flink_job.py` blind.
2. **JDBC connector + Postgres driver + Kafka connector JAR versions in
   `Dockerfile` are unverified** against Maven Central (no network access
   while writing this).
3. **Postgres credentials** — `SPC_PG_USERNAME`/`SPC_PG_PASSWORD` env vars,
   never hardcoded. Have to come from you directly.
4. **`sql/schema.sql` not yet applied** to the real database — need
   credentials first, then review before applying.
5. **This job hasn't been run against the real Kafka topic yet** — only
   manually sampled via a console consumer from a RustDesk session. Actually
   submitting the job needs network access from wherever it runs to both
   brokers (see "Kafka access" above).

## What's explicitly not here

- The hopper feedback controller (EPC) — needs a real gain (`g`)
  measurement first.
- A `raw`-estimator detector variant — an ablation found it promising, but
  only on a reduced grid.
- The 3 production lines (`ANRITSU54-1`/`YAMATO-1`/`YAMATO-2`) — deferred,
  see above. Mapping kept, calibration not.
