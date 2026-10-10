# Data dictionary (DRAFT)

Derived from `db/00_schemas.sql` … `db/04_audit.sql` as created in PostgreSQL 16. Types are PostgreSQL types.
`NN` = NOT NULL. Where a column has a closed set of values, the set is enforced by a `CHECK` constraint.

Schemas: `bronze` (raw), `silver` (validated), `gold` (analytics), `audit` (operations).

---

## bronze — immutable raw data

Both tables forbid `UPDATE` and `DELETE` with a trigger (`bronze.forbid_change()`). Everything the source sent is kept as `TEXT`
plus the original JSON; nothing is cleaned here.

### bronze.measurements — one row per API measurement (as received)
| Column | Type | Meaning |
|---|---|---|
| bronze_id | bigint NN, identity, PK | Arrival order; "earliest bronze_id wins" in Silver deduplication |
| sensor_id | text | From the request path (the payload does not carry it) |
| parameter_id | text | `parameter.id` from the payload |
| value_raw | text | The PM2.5 value as received |
| unit_raw | text | `parameter.units` as received |
| period_label | text | `period.label` (e.g. `raw`) |
| period_interval_raw | text | `period.interval` (e.g. `00:15:00`) |
| period_from_raw | text | `period.datetimeFrom.utc` as sent |
| period_to_raw | text | `period.datetimeTo.utc` as sent |
| has_flags_raw | text | `flagInfo.hasFlags` as sent |
| raw_json | jsonb NN | The complete measurement object |
| source_url | text NN | API request that produced the row (never contains the API key) |
| run_id | uuid NN | Pipeline run that loaded the row |
| ingested_at | timestamptz NN | When this pipeline received the row |
| row_hash | text NN, UNIQUE | SHA-256 of sensor id + canonical payload; drives `ON CONFLICT DO NOTHING` |

### bronze.sensors — sensor/location snapshot (one row per sensor per distinct content)
| Column | Type | Meaning |
|---|---|---|
| bronze_id | bigint NN, identity, PK | Arrival order; the latest snapshot per sensor wins in `silver.sensors` |
| sensor_id, sensor_name | text | Sensor identity |
| parameter_id, parameter_name, units | text | What the sensor measures |
| location_id, location_name, locality | text | Where it is installed (`locality` is mostly NULL in the source) |
| provider_name, country_code, timezone | text | Data provider, country, IANA timezone |
| latitude, longitude | text | Coordinates as sent |
| is_monitor, is_mobile | text | Source flags |
| datetime_first_utc, datetime_last_utc | text | Source activity range (stored, excluded from the hash because it changes every run) |
| raw_json | jsonb NN | Location object plus the sensor |
| source_url, run_id, ingested_at, row_hash | text / uuid / timestamptz / text (UNIQUE) NN | Lineage, as above |

---

## silver — validated, typed, deduplicated

### silver.sensors — sensor dimension (latest Bronze snapshot per sensor)
| Column | Type | Meaning |
|---|---|---|
| sensor_id | bigint NN, PK | |
| location_id | bigint NN | |
| location_name, provider_name, timezone | text | |
| latitude, longitude | numeric | NULL if out of range; CHECK -90..90 / -180..180 |
| is_monitor | boolean | |
| parameter_id | smallint NN | CHECK = 2 (PM2.5) |
| source_bronze_id | bigint NN | FK → bronze.sensors(bronze_id) |

### silver.measurements — VALID readings only
| Column | Type | Meaning |
|---|---|---|
| bronze_id | bigint NN, PK | FK → bronze.measurements; one Silver row ↔ one Bronze row |
| sensor_id | bigint NN | FK → silver.sensors |
| parameter_id | smallint NN | CHECK = 2 |
| period_from_utc | timestamptz NN | Canonical event time (start of the measured period) |
| period_to_utc | timestamptz NN | End of the period; CHECK > period_from_utc |
| duration_seconds | integer, generated | `period_to_utc − period_from_utc` in seconds |
| pm25_ugm3 | numeric NN | PM2.5 in µg/m³; CHECK ≥ 0 |
| is_high_value | boolean NN | Soft flag: value > 500 |
| is_long_gap | boolean NN | Soft flag: gap before this reading > 6 hours |
| is_flatline | boolean NN | Soft flag: ≥ 8 identical consecutive readings spanning ≥ 4 hours |
| gap_before_seconds | bigint | This start minus the previous end for the same sensor |
| flatline_run_length | integer | Run length, only set when `is_flatline` |
| silver_run_id | uuid NN | Run that classified the row |
| loaded_at | timestamptz NN | |

Unique key: `(sensor_id, parameter_id, period_from_utc)`.

### silver.rejected_records — Bronze rows that failed a hard rule
| Column | Type | Meaning |
|---|---|---|
| rejected_id | bigint NN, identity, PK | |
| bronze_id | bigint NN, UNIQUE | FK → bronze.measurements |
| reason_code | text NN | Primary (highest-priority) rule: `NULL_SENSOR`, `MISSING_VALUE`, `UNPARSEABLE_VALUE`, `UNPARSEABLE_TS`, `NEGATIVE_PM25`, `FUTURE_TIMESTAMP`, `ORPHAN_SENSOR`, `WRONG_PARAMETER`, `UNEXPECTED_UNIT`, `INVALID_PERIOD` |
| all_reason_codes | text[] NN | Every rule that fired, in priority order |
| reason_detail | text | The offending raw value(s) |
| silver_run_id, rejected_at | uuid NN, timestamptz NN | |

### silver.bronze_classification — exactly one verdict per Bronze row
| Column | Type | Meaning |
|---|---|---|
| bronze_id | bigint NN, PK | FK → bronze.measurements |
| classification | text NN | `VALID`, `REJECTED` or `DUPLICATE` |
| reason_code | text | Required iff REJECTED |
| duplicate_of_bronze_id | bigint | Required iff DUPLICATE; FK → silver.measurements |
| value_differs | boolean | DUPLICATE only: did this copy carry a different value than the winner? |
| silver_run_id | uuid NN | |
| classified_at | timestamptz NN | |

---

## gold — analytics (rebuilt from Silver; no foreign keys into Silver)

All Gold rows carry `gold_run_id` (the last run that **changed** the row) and `built_at`.

### gold.hourly_sensor_pm25 — one row per sensor per local hour with data
PK `(sensor_id, hour_start_utc)`.
| Column | Type | Meaning |
|---|---|---|
| sensor_id | bigint NN | |
| hour_start_utc | timestamptz NN | Instant of the local hour start (`:30` UTC for Asia/Kolkata) |
| local_hour_start | timestamp NN | Local hour of the reading's **start** |
| local_date | date NN | |
| location_id, timezone | bigint NN, text NN | |
| avg_pm25 | numeric(10,3) NN | Duration-weighted mean over all valid readings |
| avg_pm25_excl_flatline | numeric(10,3) | Same, excluding flatline readings; NULL if all are flagged |
| min_pm25, max_pm25 | numeric NN | |
| n_readings | integer NN | CHECK ≥ 1 |
| reading_interval_seconds | integer NN | Modal reading duration in the hour |
| covered_seconds | integer NN | `LEAST(3600, sum of durations)` |
| coverage_pct | numeric(5,2) NN | `100 × covered_seconds / 3600`, 0..100 |
| n_flatline_readings, n_high_value_readings | integer NN | Flag counts |

### gold.daily_location_summary — one row per location per local date with data
PK `(location_id, local_date)`.
| Column | Type | Meaning |
|---|---|---|
| location_id, local_date | bigint NN, date NN | |
| location_name, timezone, latitude, longitude | text, text NN, numeric, numeric | Taken from the location's lowest sensor id |
| avg_pm25 | numeric(10,3) NN | Mean of location-hour means (each sensor weighted equally within an hour) |
| avg_pm25_excl_flatline | numeric(10,3) | Same excluding flatline; NULL if every hour is flagged |
| min_hourly_avg, max_hourly_avg | numeric(10,3) NN | Over location-hour means |
| max_reading | numeric NN | Highest single reading (extremes stay visible) |
| n_sensors_reporting, n_sensors_at_location | integer NN | |
| n_readings, n_location_hours | integer NN | |
| expected_seconds | integer NN | Local-day length clipped to the data horizon |
| covered_seconds | integer NN | |
| coverage_pct | numeric(5,2) NN | 0..100 |
| is_partial_day | boolean NN | Day shorter than a full local day because of the horizon |
| meets_completeness | boolean NN | `coverage_pct ≥` the completeness parameter (default 75) |
| n_flatline_readings, n_high_value_readings | integer NN | |

### gold.sensor_health — one row per sensor per local day, for ALL sensors
PK `(sensor_id, local_date)`.
| Column | Type | Meaning |
|---|---|---|
| sensor_id, local_date | bigint NN, date NN | |
| location_id, timezone | bigint NN, text NN | |
| expected_interval_seconds | integer | Modal duration that day, else the sensor's all-history mode; NULL if it never reported |
| expected_seconds | integer NN | Local-day length clipped to the horizon |
| expected_readings | integer | `round(expected_seconds / expected_interval_seconds)`; NULL if the interval is unknown |
| received_readings | integer NN | |
| covered_seconds | integer NN | |
| coverage_pct | numeric(5,2) NN | |
| longest_gap_minutes | numeric(10,1) NN | Longest uncovered stretch, found on the whole series and clipped to the day |
| n_flatline_readings, n_high_value_readings, n_long_gap_readings | integer NN | |
| flatline_pct | numeric(5,2) | NULL when nothing was received |
| first_reading_utc, last_reading_utc | timestamptz | Earliest / latest reading start that day |
| is_partial_day | boolean NN | |
| health_status | text NN | `NO_DATA`, `STUCK`, `LOW_COVERAGE`, `PARTIAL`, `OK` |

---

## audit — operations and observability

### audit.pipeline_runs — one row per run
| Column | Type | Meaning |
|---|---|---|
| run_id | uuid NN, PK | Ties Bronze, Silver, Gold and audit rows together |
| pipeline_name | text NN | |
| status | text NN | `RUNNING`, `SUCCESS`, `FAILED` (only one RUNNING run is allowed) |
| stage | text NN | `start`, `bronze`, `silver`, `gold`, `reconcile`, `done` |
| window_from_utc, window_to_utc | timestamptz NN | The window that was **requested** |
| effective_horizon_from_utc, effective_horizon_to_utc | timestamptz | The horizon Gold used (window widened to cover overlapping periods) |
| observed_from_utc, observed_to_utc | timestamptz | Span of periods the API actually returned |
| started_at, finished_at | timestamptz | `finished_at` is NULL iff RUNNING |
| duration_seconds | numeric, generated | `finished_at − started_at` |
| requested_sensor_count, skipped_inactive_count, attempted_sensor_count, successful_sensor_count, failed_sensor_count | integer | Sensor accounting |
| bronze_rows_fetched, bronze_rows_inserted, bronze_rows_skipped | bigint | Skipped = already present (same hash) |
| silver_valid, silver_rejected, silver_duplicate | bigint | Rows classified **by this run** (NULL on FAILED) |
| gold_hourly_rows, gold_daily_rows, gold_health_rows | bigint | Table totals after the build |
| gold_rows_written, gold_rows_deleted | bigint | Rows this run changed |
| parameters, details | jsonb NN | Run options; stage details |
| error_message | text | Required iff FAILED |
| created_at | timestamptz NN | |

### audit.fetch_attempts — one row per HTTP attempt
| Column | Type | Meaning |
|---|---|---|
| attempt_id | bigint NN, identity, PK | |
| run_id | uuid NN | FK → pipeline_runs |
| endpoint | text NN | `locations` or `sensors/<id>/measurements` |
| sensor_id, parameter_id, page | bigint, smallint, integer | NULL for the discovery call |
| request_window_from, request_window_to | timestamptz | The window requested by this call. For sensor requests this is one 30-day slice (a slice after the first starts one hour early), not the run's overall window |
| request_url | text NN | Never contains the API key |
| attempt_no | integer NN | 1 = first try |
| started_at, finished_at | timestamptz NN | |
| duration_ms | integer | |
| outcome | text NN | `SUCCESS`, `HTTP_5XX`, `HTTP_4XX`, `RATE_LIMITED`, `TIMEOUT` (a client-side timeout or an HTTP 408; the status is kept in `http_status`), `NETWORK_ERROR`, `INVALID_RESPONSE` |
| http_status, rows_returned | integer | |
| error_type, error_message | text | Redacted |
| backoff_seconds | numeric | Sleep before the next retry |
| is_final | boolean NN | Last attempt of this request |

### audit.sensor_fetch_status — one row per discovered sensor per run
PK `(run_id, sensor_id)`.
| Column | Type | Meaning |
|---|---|---|
| run_id, sensor_id | uuid NN, bigint NN | |
| location_id, provider_name | bigint, text | |
| status | text NN | `NOT_ATTEMPTED`, `SKIPPED_INACTIVE`, `FETCHED`, `ZERO_ROWS`, `FAILED` |
| reason | text | Why (skipped, excluded, empty, error) |
| pages, http_attempts, rows_fetched | integer NN | |
| first_period_from_utc, last_period_to_utc | timestamptz | Observed data span for the sensor |
| last_http_status | integer | |
| error_type, error_message | text | |
| updated_at | timestamptz NN | |

### audit.reconciliation_checks — one row per check per run
Unique `(run_id, check_name)`.
| Column | Type | Meaning |
|---|---|---|
| check_id | bigint NN, identity, PK | |
| run_id | uuid NN | FK → pipeline_runs |
| check_name | text NN | |
| severity | text NN | `CRITICAL` or `WARNING` |
| expected_value, actual_value | numeric | |
| status | text NN | `PASS`, `FAIL`, `WARN` |
| details | text | |
| checked_at | timestamptz NN | |

### Views
| View | Purpose |
|---|---|
| audit.v_latest_sensor_fetch | Each sensor's most recent attempted outcome from a SUCCESS run |
| audit.v_provider_freshness | Per run and provider: sensors by outcome, newest data, hours behind the window end |
| audit.v_no_data_reason | Every Gold `NO_DATA` sensor-day with a reason: `FETCH_FAILED`, `SOURCE_RETURNED_ZERO_ROWS`, `NOT_ATTEMPTED`, `SKIPPED_INACTIVE`, `PROVIDER_STOPPED_DELIVERING`, `SENSOR_STOPPED_DELIVERING`, `GAP_WITHIN_SENSOR_DATA`, `NO_FETCH_RECORD` |

### Functions
| Function | Purpose |
|---|---|
| bronze.forbid_change() | Trigger function that blocks UPDATE/DELETE on Bronze |
| silver.run_silver(run_id, full_refresh, thresholds…) | Classifies new Bronze rows and recomputes soft flags in one transaction |
| gold.run_gold(run_id, thresholds…, horizon_from, horizon_to) | Rebuilds the three Gold tables from Silver with diff-based upserts |
| audit.reconcile(run_id, stats) | Computes the 25 reconciliation checks and returns them (writes nothing) |
