# Engineering decisions (DRAFT)

Each entry is something that is actually implemented. Format: **decision → why → trade-off → where**.

## 1. Bronze is immutable raw data
* **Decision.** Bronze stores values as `TEXT` plus the original JSON and the request that produced them; a trigger rejects `UPDATE` and `DELETE`.
* **Why.** If a validation rule turns out to be wrong you fix the rule and replay from Bronze. If Bronze had been "cleaned", the original evidence would be gone.
* **Trade-off.** More storage, and corrections from the source arrive as additional rows rather than edits. `TRUNCATE` is not blocked on purpose (development resets); production would revoke those privileges.
* **Where.** `db/01_bronze.sql`, `pipeline/ingest_bronze.py`.

## 2. Content hash for idempotent loading
* **Decision.** `row_hash` = SHA-256 of the sensor id and the canonical payload; unique constraint + `ON CONFLICT DO NOTHING` via a staging table filled with `COPY`.
* **Why.** Reloading an overlapping window must insert nothing. Volatile fields (run id, ingestion time, location `datetimeLast`) are kept out of the hash, otherwise every rerun would look new.
* **Trade-off.** A source-side correction produces a new Bronze row, which Silver then treats as a duplicate (see 3).
* **Where.** `pipeline/ingest_bronze.py`.

## 3. Silver hard validation rules
* **Decision.** Ten hard rules, evaluated in a fixed priority order: `NULL_SENSOR`, `MISSING_VALUE`, `UNPARSEABLE_VALUE`, `UNPARSEABLE_TS`, `NEGATIVE_PM25`, `FUTURE_TIMESTAMP`, `ORPHAN_SENSOR`, `WRONG_PARAMETER`, `UNEXPECTED_UNIT`, `INVALID_PERIOD`. The first rule that fires is the reason code; all that fired are kept.
* **Why.** Every Bronze row must end with an explained verdict. Parsing uses a strict regex *before* the cast, because Postgres' own validators accept `now`, `NaN`, `Infinity` and `1e3`. The unit is compared by exact code point (µ U+00B5, not Greek mu U+03BC).
* **Trade-off.** Values outside the bounded numeric shape (≤10 integer digits, ≤6 decimals) are rejected as unparseable rather than risking an overflow.
* **Where.** `db/02_silver.sql` (`silver.run_silver`).

## 4. Reject before deduplicate
* **Decision.** Deduplication only considers rows that passed every hard rule; the lowest `bronze_id` wins.
* **Why.** A broken row must never claim a natural key and make a later good row look like a duplicate.
* **Trade-off.** "Keep earliest" keeps the original value when the source later corrects it; such cases are visible as `value_differs` on the DUPLICATE verdict. In incremental mode "earliest" means earliest *processed*; `--full-refresh` restores strict ordering.
* **Where.** `db/02_silver.sql`.

## 5. Soft flags annotate, they do not delete
* **Decision.** `HIGH_VALUE` (> 500), `LONG_GAP` (> 6 h), `FLATLINE` (≥ 8 identical readings spanning ≥ 4 h, runs broken by gaps) never change a verdict.
* **Why.** An extreme pollution reading is not automatically invalid, and a frozen value is a signal about the sensor rather than proof the data is wrong. Thresholds are calibrated on the data and are function parameters.
* **Where.** `db/02_silver.sql`.

## 6. Deterministic rules
* **Decision.** Rules depend on the data only (e.g. `FUTURE_TIMESTAMP` compares with `ingested_at`, not `now()`).
* **Why.** Reprocessing the same Bronze must always give the same verdicts.

## 7. Local-time bucketing with start-attribution
* **Decision.** A reading belongs to the local hour and local date in which its `period_from_utc` falls. `hour_start_utc` is that local hour as an instant (`:30` UTC for Asia/Kolkata).
* **Why.** Daily summaries must follow the location's timezone rather than a UTC cut. Start-attribution keeps every Silver row in exactly one Gold hour, so counts reconcile exactly.
* **Trade-off.** UTC-aligned hourly sensors (local `:30`) are smeared by up to 30 minutes. Pro-rata allocation would be exact but breaks one-row lineage.
* **Where.** `db/03_gold.sql`.

## 8. Duration-weighted hourly aggregation
* **Decision.** `avg = Σ(value × duration) / Σduration`; coverage is measured in seconds, not reading counts.
* **Why.** 5-, 15- and 60-minute sensors become comparable, and an hour mixing durations is still correct. Nothing assumes 96 readings per day.
* **Where.** `db/03_gold.sql`.

## 9. Two-stage daily aggregation
* **Decision.** Sensor-hour means are averaged per location-hour with equal weight per sensor, then location-hour means are averaged over the day.
* **Why.** A location can have two sensors; a 15-minute sensor must not outweigh an hourly one, and an hour must count once however many sensors reported it.
* **Where.** `db/03_gold.sql`.

## 10. FLATLINE is kept in the main average
* **Decision.** `avg_pm25` includes flatline readings; `avg_pm25_excl_flatline` is stored beside it; counts are always exposed.
* **Why.** Excluding them would erase whole sensor-hours (on the 7-day data, 576) and change reconciliation. Keeping them and exposing the flag lets analysts choose.

## 11. Sensor health covers every sensor
* **Decision.** `gold.sensor_health` has one row per sensor per local day for *all* sensors, including those that never reported (`NO_DATA`, NULL expected interval).
* **Why.** A health table that only lists sensors with data cannot show dead sensors. Expected readings are derived from each sensor's own interval and from the horizon-clipped day length.
* **Where.** `db/03_gold.sql`.

## 12. Gold has no foreign keys into Silver
* **Decision.** Integrity is enforced by reconciliation guards instead.
* **Why.** Silver's `--full-refresh` truncates `silver.sensors`; Postgres refuses to truncate a referenced table. A test proves Silver full refresh still works after Gold exists.

## 13. Gold is recomputed and applied as differences
* **Decision.** Full recompute into temporary tables, `INSERT … ON CONFLICT DO UPDATE … WHERE … IS DISTINCT FROM`, then delete stale keys; one transaction.
* **Why.** Always converges to what Silver implies, and an unchanged rerun writes zero rows and keeps `gold_run_id` meaningful.
* **Trade-off.** Cost grows with total Silver rather than the delta; restricting recompute to touched sensors is a known future improvement.

## 14. A durable audit layer with one run id
* **Decision.** `audit.pipeline_runs`, `fetch_attempts`, `sensor_fetch_status`, `reconciliation_checks`; the same `run_id` is written to Bronze, Silver, Gold and audit rows.
* **Why.** It answers what was requested, fetched, failed and reconciled. Audit rows go through a separate autocommit connection so evidence survives a rollback.
* **Where.** `db/04_audit.sql`, `pipeline/audit.py`.

## 15. Explicit ingestion window
* **Decision.** The requested window is stored in `pipeline_runs` and handed to Gold, widened only as far as needed to cover Silver data.
* **Why.** Coverage must be measured against what was asked for, not inferred from the data. The API returns periods that overlap the window edge, so the widening is done by the orchestrator and Gold's own "horizon must cover the data" check stays strict.

## 16. Failure tolerance
* **Decision.** A sensor whose request fails after retries is recorded and the run continues; the run fails if more than `--max-failed-fraction` (default 5 %) of attempted sensors fail. Any other exception aborts the run.
* **Why.** A handful of persistently failing sensors should not block the pipeline, but they must be visible and bounded. Swallowing every exception per sensor would hide database errors, so only `FetchError` is tolerated. HTTP 408 is classified as a timeout and retried like any other transient failure (see 20 for why that alone was not enough).

## 17. Advisory locking
* **Decision.** A session advisory lock for the whole run, transaction-level locks for Silver and Gold (Gold takes Silver's first), and a unique partial index so the database allows only one `RUNNING` run.
* **Why.** Prevents overlapping runs and Gold reading a half-built Silver. A crashed run's stale `RUNNING` row is cleaned up by the next run.

## 18. Idempotent processing end to end
* Bronze by content hash; Silver by "classify only what has no verdict"; Gold by diff-based upserts; audit by a new run row per run. A second run over the same window changes nothing but is fully audited.

## 19. Tests that can fail
* **Decision.** The suite runs on an isolated database, uses a scripted fake API with the real client, includes an independent Python oracle for Gold, and is mutation-tested (rules are deliberately broken to confirm the tests notice).
* **Why.** A validation layer that always reports "all clean" looks identical to one that is broken; dirty fixtures and mutation checks are what prove the rules fire.

## 20. Fetch each sensor's window in 30-day time slices
* **Context.** The 90-day load left four sensors incomplete: page 5-7 of a 90-day request returned HTTP 408 (Request Timeout). Treating 408 as retryable was correct but did not fix it: a re-fetch got 408 on all five attempts, because the API paginates by offset and deep pages are deterministically slow. Direct API calls confirmed that the same rows come back quickly as shallow pages of a smaller window.
* **Decision.** Split each sensor's requested window into consecutive slices of at most 30 days (2-3 pages each). Every slice after the first starts one hour early.
* **Why it is safe.** The row hash depends only on content, never on the request, so an overlapping row is skipped as already present (no duplicate Bronze row, verified against five hypothetical API boundary rules, because the real rule is undocumented). The run still records the overall requested window; `fetch_attempts` records the slices.
* **Trade-off.** A few rows per slice boundary are fetched twice (27 extra rows across four sensors in the corrective run). A failing slice fails the sensor immediately rather than trying its later slices, which bounds the cost of sensors that always error.
* **Result.** The corrective run fetched the four sensors with 35 requests, all HTTP 200, deepest page 3, and recovered 3,442 rows; sensor 12235391 went from 4,336 to 5,781 rows, exactly what the API holds for it.
* **Where.** `pipeline/ingest_bronze.py` (`time_slices`, `SLICE_DAYS`, `SLICE_OVERLAP`), `tests/test_slicing.py`.
