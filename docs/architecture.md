# Architecture (DRAFT)

Describes what is implemented today. Numbers from a specific run are intentionally left out; they belong in the README results section.

## Overview

```text
                         OpenAQ v3 API  (India, PM2.5 = parameter 2, country id 9)
                                  │  HTTP, X-API-Key header, retries + rate-limit handling
                                  ▼
  ┌──────────────────────────────────────────────────────────────────────────────────────┐
  │ pipeline/run_all.py   one run_id for everything                                      │
  │                                                                                      │
  │  BRONZE   (committed chunk by chunk, never rolled back)                              │
  │    bronze.sensors, bronze.measurements   raw TEXT + JSON, immutable, content-hashed  │
  │                                                                                      │
  │  ┌───────────────────── ONE TRANSACTION ───────────────────────────────────────┐    │
  │  │ SILVER   silver.sensors, silver.measurements, silver.rejected_records,        │    │
  │  │          silver.bronze_classification   (VALID | REJECTED | DUPLICATE)        │    │
  │  │ GOLD     gold.hourly_sensor_pm25, gold.daily_location_summary,                │    │
  │  │          gold.sensor_health                                                   │    │
  │  │ RECONCILE  audit.reconcile()  →  25 checks;  any CRITICAL FAIL ⇒ ROLLBACK     │    │
  │  └───────────────────────────────────────────────────────────────────────────────┘    │
  └───────────────────────────────┬──────────────────────────────────────────────────────┘
                                  │ separate autocommit connection: evidence survives rollbacks
                                  ▼
          AUDIT   audit.pipeline_runs · fetch_attempts · sensor_fetch_status · reconciliation_checks
                  views: v_latest_sensor_fetch · v_provider_freshness · v_no_data_reason
```

## Layers

| Layer | Schema | Contract |
|---|---|---|
| Bronze | `bronze` | What the source said, untouched. Everything is `TEXT` plus the original JSON; rows are content-hashed (`row_hash` UNIQUE) so reloading the same data inserts nothing. A trigger blocks `UPDATE` and `DELETE`. |
| Silver | `silver` | Typed, validated, deduplicated. Each Bronze row receives exactly one verdict. Soft quality flags annotate VALID rows without removing them. |
| Gold | `gold` | Analytics tables derived only from Silver, rebuilt on every run and applied as differences. Disposable by design. |
| Audit | `audit` | What was requested, what was fetched, what failed, and whether every layer reconciles. |

## Run lifecycle (`pipeline/run_all.py`)

1. Acquire a session-level advisory lock (`audit.run_all`). If it is held, stop with `PipelineBusy`. Mark any stale `RUNNING` row (a crashed process) as `FAILED (abandoned)`.
2. Insert the run row (`RUNNING`) with the **requested window** exactly as given.
3. **Bronze** — discover India PM2.5 sensors, snapshot them, then fetch each sensor's measurements for the window. Every HTTP attempt is recorded (`fetch_attempts`) and every sensor ends with a status (`sensor_fetch_status`). Only `FetchError` is treated as a per-sensor failure; any other exception aborts the run.
4. **Silver** — `silver.run_silver(run_id)` classifies Bronze rows that have no verdict yet (`NOT EXISTS`, not a high-water mark) and recomputes soft flags for touched sensors.
5. **Gold** — the requested window is widened just enough to cover Silver data (the API returns periods that overlap the window edge) and passed to `gold.run_gold(run_id, horizon…)`.
6. **Reconcile** — `audit.reconcile(run_id, stats)` returns the checks. If any `CRITICAL` check fails, the Silver/Gold transaction is **rolled back**; otherwise it commits.
7. Record the checks, then finish the run as `SUCCESS` or `FAILED` with an error message. A failure re-raises as `PipelineFailed` with the original exception chained; Ctrl-C is recorded but still propagates.

## Transaction and concurrency model

| Scope | Behaviour |
|---|---|
| Bronze | Each page of rows commits on its own. Raw data is append-only and is never rolled back. |
| Silver + Gold + reconciliation | One transaction on one connection. Atomic: all or nothing. |
| Audit | A second, autocommit connection, so a failed run still leaves its fetch attempts, statuses and failed checks. |
| Locks | `audit.run_all` (session) guards the whole run. `silver.run_silver` and `gold.run_gold` take transaction-level advisory locks; Gold takes Silver's first, so it can never read a half-finished Silver run. A unique partial index allows only one `RUNNING` run at the database level. |

## Idempotency

| Layer | Mechanism |
|---|---|
| Bronze | `row_hash` unique + `INSERT … ON CONFLICT DO NOTHING` through a staging table; the hash covers content, never run id or timestamps. |
| Silver | Only unclassified Bronze rows are processed; `--full-refresh` rebuilds from Bronze. |
| Gold | Full recompute into temporary tables, then diff-based upsert (`IS DISTINCT FROM`) and delete of stale keys. A rerun with unchanged data writes zero rows and leaves `gold_run_id` untouched. |

## Failure model

* A sensor whose request cannot be completed is **recorded** (`FAILED`, with every attempt) and the run continues; the run fails only if the failure rate exceeds `--max-failed-fraction` (default 5 %).
* Silver and Gold failures (including the in-database guards) roll back and are recorded as `FAILED` with the failing stage.
* The API key is sent only in a header; the client and the audit writer both redact it from anything stored or logged.

## Explaining "no data"

Gold marks a sensor-day `NO_DATA`. `audit.v_no_data_reason` attributes it using the last successful fetch evidence: the request failed, the source returned zero rows, the sensor was skipped or never attempted, the sensor stopped delivering, or its whole provider stopped.

## Testing architecture

Tests run against a throw-away `openaq_test` database rebuilt from the same `db/*.sql` files (a guard refuses to run if its name equals the configured database). The HTTP layer is replaced by a scripted fake session injected into the real client, so retry, classification and audit code all run for real without network access. An independent Python re-implementation of the Gold formulas (`tests/oracle.py`) can be run against any database. Rules are further checked by mutation testing (deliberately breaking the SQL or code and confirming the suite fails).
