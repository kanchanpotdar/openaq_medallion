# OpenAQ India PM2.5 — Medallion Data Pipeline

An end-to-end, **auditable** data pipeline that ingests PM2.5 air-quality measurements for India from the
[OpenAQ v3 API](https://docs.openaq.org/) into **PostgreSQL 16**, refines them through **Bronze → Silver → Gold**
layers using set-based SQL, and records a full audit trail (requested window, every HTTP attempt, every
reconciliation check) for each run.

Stack: Python · PostgreSQL 16 · SQL (CTEs, window functions) · Docker Compose · pytest

> **Status: complete and validated at production scale.** It has been run end to end on a 90-day window
> (**3.3 million raw measurements**, 559 sensors with data, a 6.5 GB database) with 201 automated tests passing and
> every reconciliation check green. See [Production-scale validation](#production-scale-validation) and
> [Completeness: what is and is not claimed](#completeness-what-is-and-is-not-claimed).

---

## Why this project

It is built to show data-engineering fundamentals on real, messy data rather than a clean tutorial dataset:

* **SQL-first transformation** — Silver and Gold are set-based PostgreSQL (PL/pgSQL functions, CTEs, window
  functions, gaps-and-islands); Python only fetches data and orchestrates.
* **Medallion architecture** with a clear contract per layer.
* **Auditability and lineage** — one `run_id` ties Bronze, Silver, Gold and audit rows together.
* **Data quality as code** — explicit hard rules, soft flags, and a verdict for every raw row.
* **Idempotent, safely re-runnable** processing at every layer.
* **Reconciliation** — 25 durable checks; a failed critical check fails the run and rolls back Silver and Gold.
* **Operating at scale** — a real production issue (deterministic API timeouts on deep pages) was diagnosed from the
  audit trail, fixed, and verified, as described below.
* **Tests that can fail** — isolated test database, scripted fake API, an independent Python oracle, and mutation testing.

---

## Architecture

```text
                       OpenAQ v3 API   (India · PM2.5 · parameter 2 · country id 9)
                                │  X-API-Key header · retries · rate-limit handling · 30-day request slices
                                ▼
 ┌────────────────────────────────────────────────────────────────────────────────────┐
 │  pipeline/run_all.py        one run_id for the whole run                           │
 │                                                                                    │
 │  BRONZE  bronze.sensors · bronze.measurements                                      │
 │          raw TEXT + JSON, immutable, content-hashed     (committed per page)       │
 │                                                                                    │
 │  ┌─────────────────────────── ONE TRANSACTION ───────────────────────────────┐    │
 │  │ SILVER  silver.sensors · measurements · rejected_records ·                  │    │
 │  │         bronze_classification     (VALID | REJECTED | DUPLICATE per row)    │    │
 │  │ GOLD    gold.hourly_sensor_pm25 · daily_location_summary · sensor_health    │    │
 │  │ RECONCILE   audit.reconcile() → 25 checks   CRITICAL FAIL ⇒ ROLLBACK        │    │
 │  └─────────────────────────────────────────────────────────────────────────────┘    │
 └─────────────────────────────┬──────────────────────────────────────────────────────┘
                               │ separate autocommit connection: evidence survives rollbacks
                               ▼
        AUDIT  audit.pipeline_runs · fetch_attempts · sensor_fetch_status · reconciliation_checks
               views: v_latest_sensor_fetch · v_provider_freshness · v_no_data_reason
```

More detail: [docs/architecture.md](docs/architecture.md) · design rationale: [docs/decisions.md](docs/decisions.md) ·
every table and column: [docs/data_dictionary.md](docs/data_dictionary.md).

---

## Data source

OpenAQ v3, filtered to **India** (country id `9` — the `/countries?iso=IN` filter is silently ignored by the API, so the
pipeline filters by id and asserts that every returned location is `IN`) and **PM2.5** (parameter id `2`).

Things the data taught the design (all observed, see [docs/decisions.md](docs/decisions.md)):

* **Mixed reporting intervals** — 5-minute, 15-minute and hourly sensors; nothing assumes "96 readings a day".
* **Periods, not points** — each measurement has a start and an end; the start (`period_from_utc`) is the event time.
* **Overlapping window edges** — the API returns periods that begin slightly before the requested window.
* **Timezone** — all locations are `Asia/Kolkata` (UTC+5:30), so daily summaries use the local date, not the UTC date.
* **Missing metadata** — `locality` is null for almost every location, so Gold aggregates by location, not city.
* **Offset pagination** — deep result pages of a long window get slow and time out (see below).
* **Source gaps** — some providers stop delivering earlier than others, and a small set of sensors reproducibly returns HTTP 500.

---

## Bronze — immutable raw data

* Everything the API sent is stored as `TEXT` plus the original JSON, with lineage columns (`source_url`, `run_id`,
  `ingested_at`). Nothing is cleaned here.
* `row_hash` (SHA-256 of sensor id + canonical payload) is `UNIQUE`; loading uses a staging table filled with `COPY`
  followed by `INSERT … ON CONFLICT DO NOTHING`, so reloading an overlapping window inserts nothing. Volatile fields are
  kept out of the hash.
* A trigger rejects `UPDATE` and `DELETE`: source evidence is never edited.

### Fetching long windows: 30-day time slices

The first 90-day load exposed a real scale problem. The API paginates by **offset**, so page 5-7 of a 90-day request takes
about 7 seconds and is cut off with **HTTP 408 (Request Timeout)**:

1. Four sensors stopped early, each failing on a deep page. The audit trail (`fetch_attempts`) showed exactly which
   request, page and duration.
2. The client was changed to treat 408 as a **transient timeout and retry it** (it had been classified as a final client
   error).
3. **Retrying alone did not help.** A targeted re-fetch got 408 on all five attempts of the same page: the slowness is
   deterministic, not transient.
4. Direct API calls showed that the same rows come back quickly as shallow pages of a smaller window.
5. **Fix:** each sensor's window is fetched in consecutive slices of at most 30 days (2-3 pages each). Every slice after the
   first starts **one hour early**, so a period straddling a boundary cannot fall between two slices whatever the API's
   inclusive/exclusive rule is. The overlap re-fetches a few identical rows, which the existing content hash skips, so no
   duplicate Bronze row is created. The run still records the overall requested window; `fetch_attempts` records each slice.

Result: the corrective run fetched the four sensors with 35 requests, all HTTP 200 (deepest page 3, slowest 4.8 s), and
recovered **3,442 rows**; one sensor went from 4,336 to 5,781 rows, exactly the number the API holds for it.

## Silver — validated, typed, deduplicated

Every Bronze row receives **exactly one verdict** in `silver.bronze_classification`:

| Verdict | Goes to |
|---|---|
| `VALID` | `silver.measurements` (typed, with soft flags) |
| `REJECTED` | `silver.rejected_records` (reason code + offending value) |
| `DUPLICATE` | pointer to the earlier `VALID` row with the same key |

**Hard rules** (priority order; the first that fires is the reason code, all that fired are kept):
`NULL_SENSOR` · `MISSING_VALUE` · `UNPARSEABLE_VALUE` · `UNPARSEABLE_TS` · `NEGATIVE_PM25` · `FUTURE_TIMESTAMP` ·
`ORPHAN_SENSOR` · `WRONG_PARAMETER` · `UNEXPECTED_UNIT` · `INVALID_PERIOD`.

* Values are shape-checked with a strict regex **before** casting (PostgreSQL's own validators accept `now`, `NaN`,
  `Infinity` and `1e3`).
* Rows are validated **before** deduplication, so a broken row can never claim a key. Natural key:
  `(sensor_id, parameter_id, period_from_utc)`; the lowest `bronze_id` wins.
* Rules are deterministic: `FUTURE_TIMESTAMP` compares with `ingested_at`, not `now()`.

**Soft flags** annotate valid rows and never remove them: `HIGH_VALUE` (> 500 µg/m³), `LONG_GAP` (> 6 h),
`FLATLINE` (≥ 8 identical consecutive readings spanning ≥ 4 h; runs are broken by outages).

## Gold — analytics tables (built only from Silver)

| Table | Grain | Highlights |
|---|---|---|
| `gold.hourly_sensor_pm25` | sensor × local hour | duration-weighted mean, min/max, `n_readings`, time-based `coverage_pct`, flag counts, plus `avg_pm25_excl_flatline` |
| `gold.daily_location_summary` | location × local date | two-stage mean (sensor-hour → location-hour → day), expected vs covered time, `is_partial_day`, `meets_completeness` (≥ 75 %) |
| `gold.sensor_health` | sensor × local day, **all sensors** | derived expected readings (never a fixed 96), longest gap, flatline %, status `OK` / `PARTIAL` / `LOW_COVERAGE` / `STUCK` / `NO_DATA` |

Key choices: a reading belongs to the **local hour/date in which it starts**; sensors with different intervals are
comparable because coverage is measured in **seconds**; flatline readings stay in the main average and are exposed
(with an alternative average that excludes them) rather than silently dropped.

## Audit and observability

| Table / view | Answers |
|---|---|
| `audit.pipeline_runs` | what was **requested** (exact window), what horizon Gold used, what the API returned, timings, counts per layer, status |
| `audit.fetch_attempts` | every HTTP attempt: outcome (`SUCCESS`, `HTTP_5XX`, `RATE_LIMITED`, `TIMEOUT` …), status code, backoff, and the (sliced) window requested |
| `audit.sensor_fetch_status` | each sensor's fate in a run: `FETCHED`, `ZERO_ROWS`, `FAILED`, `SKIPPED_INACTIVE`, `NOT_ATTEMPTED` |
| `audit.reconciliation_checks` | the 25 checks, expected vs actual |
| `audit.v_no_data_reason` | *why* a sensor-day has no data — fetch failed, source returned nothing, skipped, or the provider/sensor stopped |
| `audit.v_provider_freshness` | how far behind the window end each provider's newest data is |

The API key is sent only in a request header and is redacted from anything stored or logged.

## Idempotency and reconciliation

* **Bronze** — content hash + `ON CONFLICT DO NOTHING`.
* **Silver** — only Bronze rows without a verdict are classified; `--full-refresh` rebuilds from Bronze.
* **Gold** — full recompute into temp tables, then diff-based upserts and stale-row deletes; an unchanged rerun writes
  **zero** rows and leaves lineage untouched.
* **Reconciliation** — includes `bronze = valid + rejected + duplicate`, every Gold layer summing back to Silver,
  every sensor × day present, coverage bounds, Silver values matching their Bronze source, Bronze immutability
  triggers enabled, and the same `run_id` present in every layer.
* **Failure model** — Bronze commits per page and is never rolled back. Silver + Gold + reconciliation run in
  **one transaction**; a failed `CRITICAL` check rolls it back. Individual sensors that fail to fetch are recorded and
  tolerated up to `--max-failed-fraction` (default 5 %). Concurrent runs are blocked by an advisory lock plus a unique
  index allowing a single `RUNNING` run.

---

## Setup

**Prerequisites:** Docker, Python 3.11+ (developed and tested on 3.14), PostgreSQL 16 (provided by Docker Compose;
the SQL uses `pg_input_is_valid`, which needs 16+), and a free OpenAQ API key from <https://explore.openaq.org>.

```bash
git clone https://github.com/kanchanpotdar/openaq_medallion.git
cd openaq_medallion

# 1. configuration — .env is git-ignored; never commit it
cp .env.example .env            # then edit .env: set OPENAQ_API_KEY and a local POSTGRES_PASSWORD

# 2. database
docker compose up -d            # PostgreSQL 16; the first start creates the four schemas

# 3. python environment
python -m venv .venv
source .venv/bin/activate       # Windows PowerShell: .venv\Scripts\Activate.ps1
pip install -r requirements.txt           # runtime only
# pip install -r requirements-dev.txt     # add this to run the tests

# 4. create all tables, functions and views (safe to re-run)
python -m pipeline.apply_sql
```

## Running the pipeline

```bash
# a pinned 7-day window (reproducible; about half an hour)
python -m pipeline.run_all --days 7 --end 2026-10-09T00:00:00Z

# the 90-day window used for the results below (about four hours, rate-limited by the API)
python -m pipeline.run_all --days 90 --end 2026-10-09T00:00:00Z

# re-fetch specific sensors for the same window (idempotent; existing rows are skipped by hash)
python -m pipeline.run_all --days 90 --end 2026-10-09T00:00:00Z --sensor-id 12235391
```

| Option | Meaning |
|---|---|
| `--days N`, `--end ISO` | window length and end (UTC); `--end` defaults to now |
| `--limit-sensors N`, `--sensor-id ID` (repeatable) | restrict which sensors are fetched |
| `--max-failed-fraction F` | fail the run if more than this share of attempted sensors fail (default `0.05`) |

Exit codes: `0` success · `1` failed (details in `audit.pipeline_runs`) · `3` another run is in progress.
Layers can also be run alone: `python -m pipeline.run_silver [--full-refresh]`, `python -m pipeline.run_gold`.

Look at the result:

```sql
SELECT run_id, status, stage, window_from_utc, window_to_utc, bronze_rows_inserted, silver_valid, gold_hourly_rows
FROM audit.pipeline_runs ORDER BY started_at DESC LIMIT 1;

SELECT check_name, status, expected_value, actual_value FROM audit.reconciliation_checks
WHERE run_id = (SELECT run_id FROM audit.pipeline_runs ORDER BY started_at DESC LIMIT 1) AND status <> 'PASS';
```

---

## Data quality

| Aspect | Behaviour |
|---|---|
| Hard rules | 10 rules, evaluated in priority order, each with a recorded reason and raw value |
| Soft flags | `HIGH_VALUE`, `LONG_GAP`, `FLATLINE` — visible, never silently dropped |
| Coverage | measured in seconds against the **requested window**, not inferred from the data |
| Partial days | edge days are marked `is_partial_day` and use a clipped expected length |
| Unexplained gaps | `v_no_data_reason` separates fetch failures from silent sensors from provider outages |

---

## Testing

```bash
pip install -r requirements-dev.txt
pytest            # runs against a throw-away `openaq_test` database
```

> The tests create and **drop** a database named `openaq_test` on the server configured in `.env` (they refuse to run
> if that name equals your configured database). Postgres must be running.

* **201 tests** across Silver rules and flags, Gold hourly/daily/health, run-level behaviour (idempotency, rollback,
  concurrency, guards), the audit layer, and the 30-day slicing.
* **Dirty fixtures** — a CSV with one row per hard rule, duplicates and priority cases is the executable spec of the Silver rules.
* **Scripted fake API** injected into the real HTTP client, so retry/classification/audit/slicing code runs without network.
  The fake can apply five different boundary rules to a requested window, so the slicing is tested against every plausible
  API behaviour.
* **Independent oracle** — `tests/oracle.py` re-implements the Gold formulas in plain Python (`Decimal` + `zoneinfo`)
  and compares **every row**: `python -m tests.oracle --dbname <database>`.
* **Mutation testing** — the SQL and code were deliberately broken one change at a time (e.g. `>` → `>=`, disabled rules,
  dropped locks, removed slice overlap) and the suite was confirmed to fail each time: **11/11 Silver, 20/20 Gold,
  22/22 audit and 10/10 slicing** mutations were caught.

---

## SQL showcase

Runnable queries against the real schemas, in [`sql/showcase/`](sql/showcase/):

| File | Demonstrates |
|---|---|
| `01_data_quality_overview.sql` | verdict breakdown, percentages via a window function, `FILTER` aggregates |
| `02_sensor_health_ranking.sql` | `RANK()` over aggregates, time-based coverage |
| `03_provider_freshness.sql` | audit view, latest-run subquery |
| `04_daily_location_pm25.sql` | named `WINDOW`, `LAG`, rolling average, per-day ranking |
| `05_incomplete_sensors_and_days.sql` | incomplete days, explained `NO_DATA` |
| `06_reconciliation.sql` | cross-layer reconciliation, non-PASS checks |
| `07_high_value_readings.sql` | `percentile_cont` in a CTE, time zones |
| `08_flatline_analysis.sql` | gaps-and-islands with `LAG` and a running `SUM` |

```bash
docker exec -i openaq_postgres psql -U openaq -d openaq < sql/showcase/01_data_quality_overview.sql
```

---

## Results

Two end-to-end runs through `pipeline.run_all` against the real API:

| Metric | 7-day run (2026-10-02 → 10-09) | 90-day run (2026-07-11 → 10-09) |
|---|---|---|
| Sensors discovered / skipped inactive / attempted | 1,067 / 211 / 856 | 1,067 / 185 / 882 |
| Sensors that failed to fetch | 15 | 19 in the first pass; **15** after the slicing fix (4 recovered) |
| Sensors with data | 543 | **559** |
| Bronze measurement rows | 209,009 | **3,315,213** (the 7-day rows are a subset) |
| Silver `VALID` / `REJECTED` / `DUPLICATE` | 209,009 / 0 / 0 | **3,315,208 / 5 / 0** |
| Silver soft flags | 2,289 flatline · 1,084 long-gap · 15 high-value | 50,799 flatline · 6,071 long-gap · 877 high-value |
| Gold hourly / daily / health rows | 58,326 / 3,182 / 8,536 | **954,904 / 45,889 / 97,097** |
| Reconciliation checks (per run: 25) | 24 pass · 1 warning · 0 fail | main run 24 pass · 1 warning · 0 fail; corrective run **25/25 pass** |
| Run time | ~27 min | ~3 h 56 min main run (rate-limited API) + 4 min corrective re-fetch |
| Database size | 429 MB | **6,551 MB** (Bronze 4.8 GB, Silver 1.5 GB, Gold 0.25 GB) |

Gold on the 90-day data: 559 locations; 45,889 location-days of which 31,748 (69 %) meet the 75 % completeness bar and
539 are partial edge days; 954,904 sensor-hours of which 754,190 are fully covered. Sensor-day health:
52.7 % `NO_DATA`, 21.9 % `PARTIAL`, 19.9 % `OK`, 5.2 % `LOW_COVERAGE`, 0.3 % `STUCK` — and every `NO_DATA` day carries a
reason (see below).

## Production-scale validation

| Check | Result |
|---|---|
| Rows processed | **3,315,213** Bronze → **3,315,208** Silver VALID (+ 5 rejected, 0 duplicate) → 954,904 / 45,889 / 97,097 Gold rows |
| Sensors with data | **559** (of 1,067 discovered) |
| Database size | **6,551 MB** |
| Automated tests | **201** passing |
| Mutation tests (slicing) | **10/10** caught |
| Reconciliation | **25/25** checks pass on the corrective run; the main run's only non-pass was the tolerated fetch-failure warning |
| Rows recovered by 30-day slicing | **3,442** across 4 sensors; all targeted API requests then succeeded (35/35, deepest page 3) |
| Duplicate natural keys | **0** in Bronze and in Silver |
| Bronze immutability | the 3,311,771 rows that existed before the corrective run are byte-identical afterwards (checksum), and content outside the four affected sensors is unchanged in every layer |
| Independent oracle | 0 mismatches across all Gold rows on the 7-day data (70,044 rows) |
| Silver + Gold + reconciliation at ~3.3M rows | ~12 minutes (Gold is a full recompute, so its cost scales with total Silver rows) |

## Completeness: what is and is not claimed

These four things are deliberately kept separate, because a pipeline can be complete without the source being complete.

* **Pipeline completeness (claimed, and verified).** Every measurement the API returned for the requested window is in
  Bronze, every Bronze row has exactly one Silver verdict, and every Gold layer sums back to Silver. This is checked by 25
  reconciliation checks per run, and each of the 1,067 discovered sensors has a recorded fetch outcome.
* **Source completeness (not claimed).** The pipeline holds what the API provides, not necessarily everything that was
  measured. 308 sensors returned zero rows for the window, 185 were skipped because the source reports them inactive, and
  many sensors have gaps within their history; these are visible as `NO_DATA` and low-coverage days rather than hidden.
* **Known source/API failures (a limitation).** **15 sensors** (12 under a provider named "N/A", 3 under CPCB) return
  HTTP 500 on every attempt, and have done so in every run so far. They are recorded as `FETCH_FAILED` in the audit trail
  (not mistaken for silent sensors) and tolerated within the 5 % failure budget. Their data is not in the dataset.
* **Provider freshness (a limitation).** The CPCB feed — and the "N/A" provider — ends at **2026-10-07 14:30 UTC, about
  33.5 hours before the window end**, so those sensors show no data for the final day and a half. The audit trail
  attributes this to the **provider** (`PROVIDER_STOPPED_DELIVERING`), not to individual sensors. AirGradient, AirNow and
  Clarity are current to within 0.5 hours.

## Data-quality finding: five rejected source rows

Over 3.3 million rows the Silver rules rejected exactly **5**, all genuine source anomalies, each with its reason and raw
value recorded in `silver.rejected_records`:

* one reading of **−4.0 µg/m³** from an AirNow sensor → `NEGATIVE_PM25`;
* four consecutive 15-minute periods with a **NULL value** from one sensor → `MISSING_VALUE`.

The 7-day window contained none of these; the larger window surfaced them. No row was duplicated (0 `DUPLICATE`).

---

## Known limitations

* **Source gaps.** See "Completeness" above: 15 sensors that always return HTTP 500, the CPCB/"N/A" feeds that end early,
  and sensors with genuine gaps or no data. The pipeline reports these; it cannot create data the API does not provide.
* **Deep-page slowness is avoided, not removed.** The API still paginates by offset. 30-day slices keep every request to a
  few pages, but a sensor reporting far more than ~3,000 readings per 30 days would need smaller slices.
* **Attribution smear.** Because a reading is attributed to the local hour in which it *starts*, UTC-aligned hourly
  sensors (local `:30`) are shifted by up to 30 minutes within their hour.
* **Gold recompute cost.** Gold is recomputed in full on every run, so its cost scales with total Silver rows.
* **Keep-earliest deduplication.** A later source-side correction of an already-loaded value is classified as a
  duplicate (visible via `value_differs`) and the original value is kept. None occurred in this dataset.
* **Fail-fast slices.** If one slice of a sensor fails after all retries, the sensor's remaining slices are not requested
  in that run (bounding the cost of always-failing sensors); a rerun with `--sensor-id` picks them up.
* **Single country and parameter.** The pipeline is parameterised for India / PM2.5 only; location-level, not city-level.
* **No CI yet.** Tests are run locally.

## Future improvements

* Recompute Gold only for sensors touched by a run.
* Fetch sensors in parallel within the API rate limit.
* Adapt slice size to observed row density.
* Persist provider freshness as a time series and alert on failed runs.
* City-level aggregation once a trustworthy geographic lookup exists.
* Continuous integration running the test suite against a Postgres service container.
