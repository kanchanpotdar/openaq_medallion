# OpenAQ India PM2.5 — Medallion Data Pipeline

> **SKELETON — not the final README.** Sections marked `TBD-90D` are placeholders for the 90-day production-scale run.
> Nothing below has been filled with numbers that were not measured.

## Project overview
<!-- 3–4 sentences: what it does, for whom, and the headline result. -->
An end-to-end, auditable data pipeline that ingests PM2.5 measurements for India from the OpenAQ v3 API into PostgreSQL, refines them through Bronze → Silver → Gold layers, and records a full audit trail of every run.

## Why this project
<!-- Which data-engineering skills it demonstrates: SQL, PostgreSQL, modelling, data quality, auditability, idempotency, testing. -->
TBD

## Architecture
<!-- Insert the diagram from docs/architecture.md. -->
See [docs/architecture.md](docs/architecture.md).

## Data source
<!-- OpenAQ v3, India (country id 9), PM2.5 (parameter 2); sensors, locations, mixed 5/15/60-minute intervals; what the API does and does not guarantee. -->
TBD

## Bronze
<!-- Immutable raw layer; lineage columns; content hash; staging + COPY; immutability trigger. -->
TBD

## Silver
<!-- Ten hard rules, reject-before-dedupe, soft flags, the VALID/REJECTED/DUPLICATE verdict per Bronze row. -->
TBD

## Gold
<!-- hourly_sensor_pm25, daily_location_summary, sensor_health; local-time bucketing; duration-weighted means; coverage. -->
TBD

## Audit and observability
<!-- pipeline_runs, fetch_attempts, sensor_fetch_status, reconciliation_checks; one run_id; why "no data" is explained. -->
TBD

## Data quality
<!-- Rules and thresholds; how flags are exposed; what happens to FLATLINE and HIGH_VALUE. -->
TBD

## Idempotency and reconciliation
<!-- How reruns are safe; the 25 checks; what fails a run; rollback behaviour. -->
TBD

## Testing
<!-- Isolated test database, fake API, dirty fixtures, independent oracle, mutation testing. Insert final test/mutation counts. -->
TBD-90D (final counts after the last run)

## SQL showcase
<!-- Index of sql/showcase/*.sql with one line each. -->
| File | Demonstrates |
|---|---|
| `01_data_quality_overview.sql` | verdict breakdown, percentages with a window function, FILTER aggregates |
| `02_sensor_health_ranking.sql` | RANK() over aggregates, time-based coverage |
| `03_provider_freshness.sql` | audit view, latest-run subquery |
| `04_daily_location_pm25.sql` | named WINDOW, LAG, rolling average, per-day ranking |
| `05_incomplete_sensors_and_days.sql` | incomplete days, explained `NO_DATA` |
| `06_reconciliation.sql` | cross-layer reconciliation, non-PASS checks |
| `07_high_value_readings.sql` | percentile_cont in a CTE, time zones |
| `08_flatline_analysis.sql` | gaps-and-islands with LAG and a running SUM |

## Setup
<!-- Prerequisites (Docker, Python 3.11+), clone, copy .env.example to .env, docker compose up -d, create venv, install requirements, apply SQL. -->
TBD

## Running the pipeline
<!-- python -m pipeline.run_all --days N --end <ISO UTC>; options; exit codes; where to look in the audit tables. -->
TBD

## Results
<!-- Fill the 90-day column after the run. The 7-day column holds verified figures from the completed 7-day run. -->

| Metric | 7-day run (verified) | 90-day run |
|---|---|---|
| Requested window | 2026-10-02 → 2026-10-09 (UTC) | TBD-90D |
| Sensors discovered / attempted / failed | 1,067 / 856 / 15 | TBD-90D |
| Bronze measurement rows | 209,009 | TBD-90D |
| Silver VALID / REJECTED / DUPLICATE | 209,009 / 0 / 0 | TBD-90D |
| Gold hourly / daily / health rows | 58,326 / 3,182 / 8,536 | TBD-90D |
| Reconciliation checks (pass / warn / fail) | 24 / 1 / 0 | TBD-90D |
| End-to-end runtime | 1,643 s | TBD-90D |
| Database size | TBD | TBD-90D |
| Automated tests | 176 passed | TBD-90D |
| Independent oracle mismatches (Gold) | 0 across 70,044 rows | TBD-90D |

## Known limitations
<!-- Be specific and honest. Draft list: -->
* TBD-90D — to be completed with findings from the 90-day run.
* Start-attribution smears UTC-aligned hourly sensors by up to 30 minutes within a local hour.
* Gold is recomputed in full on every run; cost scales with total Silver rows.
* A small, deterministic set of sensors returns HTTP 500 from the source and is recorded rather than recovered.

## Future improvements
<!-- Draft list: -->
* Recompute Gold only for sensors touched by the run.
* Parallelise fetches within the API rate limit.
* Persist provider freshness as a time series and alert on failed runs.
* City-level aggregation once a trustworthy geographic lookup exists.
