-- 04_audit.sql : operational audit - what ran, what was requested, what failed, did it reconcile.
-- Idempotent DDL. Audit rows are written on a SEPARATE autocommit connection by pipeline/audit.py,
-- so evidence of a failed run survives the rollback of the transaction that failed.
--
-- One run_id ties every layer together:
--   bronze.*.run_id  = silver.*.silver_run_id = gold.*.gold_run_id = audit.*.run_id

-- ---------------------------------------------------------------- audit.pipeline_runs

CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
    run_id                     UUID        PRIMARY KEY,
    pipeline_name              TEXT        NOT NULL DEFAULT 'openaq_india_pm25',
    status                     TEXT        NOT NULL CHECK (status IN ('RUNNING','SUCCESS','FAILED')),
    stage                      TEXT        NOT NULL DEFAULT 'start'
                               CHECK (stage IN ('start','bronze','silver','gold','reconcile','done')),
    -- the window the operator ASKED for (the truth Gold used to infer from the data)
    window_from_utc            TIMESTAMPTZ NOT NULL,
    window_to_utc              TIMESTAMPTZ NOT NULL,
    -- the horizon Gold actually used: requested window widened to cover periods that overlap its edges
    effective_horizon_from_utc TIMESTAMPTZ,
    effective_horizon_to_utc   TIMESTAMPTZ,
    -- the span of source periods actually returned by the API in this run
    observed_from_utc          TIMESTAMPTZ,
    observed_to_utc            TIMESTAMPTZ,
    started_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at                TIMESTAMPTZ,
    duration_seconds           NUMERIC GENERATED ALWAYS AS
                               (EXTRACT(EPOCH FROM (finished_at - started_at))) STORED,
    -- sensors
    requested_sensor_count     INTEGER,   -- discovered for the country/parameter
    skipped_inactive_count     INTEGER,
    attempted_sensor_count     INTEGER,
    successful_sensor_count    INTEGER,   -- FETCHED + ZERO_ROWS
    failed_sensor_count        INTEGER,
    -- Bronze (committed chunk by chunk; Bronze is never rolled back)
    bronze_rows_fetched        BIGINT,
    bronze_rows_inserted       BIGINT,
    bronze_rows_skipped        BIGINT,    -- already present (same row_hash)
    -- Silver, rows classified BY THIS RUN (NULL on FAILED: that transaction rolled back)
    silver_valid               BIGINT,
    silver_rejected            BIGINT,
    silver_duplicate           BIGINT,
    -- Gold: table totals after the build, and rows this run changed
    gold_hourly_rows           BIGINT,
    gold_daily_rows            BIGINT,
    gold_health_rows           BIGINT,
    gold_rows_written          BIGINT,    -- inserted + updated, across the three tables
    gold_rows_deleted          BIGINT,
    parameters                 JSONB       NOT NULL DEFAULT '{}'::jsonb,
    details                    JSONB       NOT NULL DEFAULT '{}'::jsonb,
    error_message              TEXT,
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_runs_window   CHECK (window_from_utc < window_to_utc),
    CONSTRAINT ck_runs_finished CHECK ((status = 'RUNNING') = (finished_at IS NULL)),
    CONSTRAINT ck_runs_error    CHECK ((status = 'FAILED')  = (error_message IS NOT NULL)),
    CONSTRAINT ck_runs_done     CHECK (status <> 'SUCCESS' OR stage = 'done')
);
-- Database-level concurrent-run protection (the advisory lock is the first line, this is the backstop).
CREATE UNIQUE INDEX IF NOT EXISTS uq_one_running_run ON audit.pipeline_runs (pipeline_name) WHERE status = 'RUNNING';
CREATE INDEX IF NOT EXISTS ix_runs_started ON audit.pipeline_runs (started_at DESC);

-- ---------------------------------------------------------------- audit.fetch_attempts

-- One row per HTTP ATTEMPT (a retried request produces several rows). No secrets are stored:
-- the API key travels in a header and is never part of request_url; messages are redacted.
CREATE TABLE IF NOT EXISTS audit.fetch_attempts (
    attempt_id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id              UUID        NOT NULL REFERENCES audit.pipeline_runs (run_id),
    endpoint            TEXT        NOT NULL,          -- e.g. 'locations', 'sensors/123/measurements'
    sensor_id           BIGINT,                        -- NULL for the discovery call
    parameter_id        SMALLINT,
    page                INTEGER,
    request_window_from TIMESTAMPTZ,
    request_window_to   TIMESTAMPTZ,
    request_url         TEXT        NOT NULL,
    attempt_no          INTEGER     NOT NULL,          -- 1 = first try
    started_at          TIMESTAMPTZ NOT NULL,
    finished_at         TIMESTAMPTZ NOT NULL,
    duration_ms         INTEGER,
    outcome             TEXT        NOT NULL CHECK (outcome IN
                        ('SUCCESS','HTTP_5XX','HTTP_4XX','RATE_LIMITED','TIMEOUT','NETWORK_ERROR','INVALID_RESPONSE')),
    http_status         INTEGER,
    rows_returned       INTEGER,
    error_type          TEXT,
    error_message       TEXT,
    backoff_seconds     NUMERIC,                       -- sleep before the next retry (NULL if none)
    is_final            BOOLEAN     NOT NULL,          -- last attempt of this request
    CONSTRAINT ck_attempt_no CHECK (attempt_no >= 1)
);
CREATE INDEX IF NOT EXISTS ix_attempts_run_sensor  ON audit.fetch_attempts (run_id, sensor_id);
CREATE INDEX IF NOT EXISTS ix_attempts_run_outcome ON audit.fetch_attempts (run_id, outcome);

-- ---------------------------------------------------------------- audit.sensor_fetch_status

-- One row per discovered sensor per run: the final fate of that sensor in that run. This is what
-- separates "returned zero rows" from "request failed" from "never attempted" from "skipped".
CREATE TABLE IF NOT EXISTS audit.sensor_fetch_status (
    run_id                UUID        NOT NULL REFERENCES audit.pipeline_runs (run_id),
    sensor_id             BIGINT      NOT NULL,
    location_id           BIGINT,
    provider_name         TEXT,
    status                TEXT        NOT NULL CHECK (status IN
                          ('NOT_ATTEMPTED','SKIPPED_INACTIVE','FETCHED','ZERO_ROWS','FAILED')),
    reason                TEXT,
    pages                 INTEGER     NOT NULL DEFAULT 0,
    http_attempts         INTEGER     NOT NULL DEFAULT 0,
    rows_fetched          INTEGER     NOT NULL DEFAULT 0,
    first_period_from_utc TIMESTAMPTZ,                 -- observed span of this sensor's returned periods
    last_period_to_utc    TIMESTAMPTZ,
    last_http_status      INTEGER,
    error_type            TEXT,
    error_message         TEXT,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, sensor_id)
);
CREATE INDEX IF NOT EXISTS ix_sfs_sensor ON audit.sensor_fetch_status (sensor_id);

-- ---------------------------------------------------------------- audit.reconciliation_checks

CREATE TABLE IF NOT EXISTS audit.reconciliation_checks (
    check_id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id         UUID        NOT NULL REFERENCES audit.pipeline_runs (run_id),
    check_name     TEXT        NOT NULL,
    severity       TEXT        NOT NULL CHECK (severity IN ('CRITICAL','WARNING')),
    expected_value NUMERIC,
    actual_value   NUMERIC,
    status         TEXT        NOT NULL CHECK (status IN ('PASS','FAIL','WARN')),
    details        TEXT,
    checked_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_check_per_run UNIQUE (run_id, check_name)
);
CREATE INDEX IF NOT EXISTS ix_checks_status ON audit.reconciliation_checks (run_id, status);

-- ---------------------------------------------------------------- reconciliation function

-- Computes every check and RETURNS them (it writes nothing). The orchestrator decides whether the
-- transaction commits, then records the results durably. Call with no stats to re-verify a finished
-- run from its stored counts; run_all passes live counts (p_stats keys = pipeline_runs column names).
CREATE OR REPLACE FUNCTION audit.reconcile(p_run_id uuid, p_stats jsonb DEFAULT '{}'::jsonb)
RETURNS TABLE (check_name text, severity text, expected_value numeric, actual_value numeric,
               status text, details text)
LANGUAGE plpgsql AS $$
DECLARE
    r audit.pipeline_runs%ROWTYPE;
    v_h0 timestamptz; v_h1 timestamptz; v_w0 timestamptz; v_w1 timestamptz;
    v_requested bigint; v_attempted bigint; v_failed bigint;
    v_fetched bigint; v_inserted bigint; v_skipped bigint; v_classified bigint; v_gold_written bigint;
    v_max_frac numeric;
    v_bronze bigint; v_class bigint; v_valid bigint; v_rej bigint; v_dup bigint;
    v_silver_rows bigint; v_rej_rows bigint; v_unclassified bigint; v_dup_bad bigint;
    v_key_dupes bigint; v_mismatch bigint; v_triggers bigint;
    v_s_flat bigint; v_s_high bigint;
    v_h_n bigint; v_h_flat bigint; v_h_high bigint; v_d_n bigint; v_e_n bigint; v_e_flat bigint; v_e_high bigint;
    v_health_rows bigint; v_expected_health bigint; v_cov_bad bigint; v_zero_hours bigint;
    v_b_run bigint; v_s_run bigint; v_g_run bigint; v_status_rows bigint; v_window_ok int;
    v_failed_list text;
BEGIN
    SELECT * INTO r FROM audit.pipeline_runs WHERE run_id = p_run_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'audit.reconcile: unknown run_id %', p_run_id;
    END IF;

    v_w0 := r.window_from_utc;  v_w1 := r.window_to_utc;
    v_h0 := COALESCE((p_stats->>'effective_horizon_from_utc')::timestamptz, r.effective_horizon_from_utc);
    v_h1 := COALESCE((p_stats->>'effective_horizon_to_utc')::timestamptz,   r.effective_horizon_to_utc);
    v_requested := COALESCE((p_stats->>'requested_sensor_count')::bigint, r.requested_sensor_count);
    v_attempted := COALESCE((p_stats->>'attempted_sensor_count')::bigint, r.attempted_sensor_count);
    v_failed    := COALESCE((p_stats->>'failed_sensor_count')::bigint,    r.failed_sensor_count);
    v_fetched   := COALESCE((p_stats->>'bronze_rows_fetched')::bigint,    r.bronze_rows_fetched);
    v_inserted  := COALESCE((p_stats->>'bronze_rows_inserted')::bigint,   r.bronze_rows_inserted);
    v_skipped   := COALESCE((p_stats->>'bronze_rows_skipped')::bigint,    r.bronze_rows_skipped);
    v_classified := COALESCE((p_stats->>'silver_classified')::bigint,
                             r.silver_valid + r.silver_rejected + r.silver_duplicate);
    v_gold_written := COALESCE((p_stats->>'gold_rows_written')::bigint, r.gold_rows_written);
    v_max_frac  := COALESCE((p_stats->>'max_failed_fraction')::numeric,
                            (r.parameters->>'max_failed_fraction')::numeric, 0.05);

    -- Bronze -> Silver
    SELECT count(*) INTO v_bronze FROM bronze.measurements;
    SELECT count(*), count(*) FILTER (WHERE classification = 'VALID'),
           count(*) FILTER (WHERE classification = 'REJECTED'),
           count(*) FILTER (WHERE classification = 'DUPLICATE')
      INTO v_class, v_valid, v_rej, v_dup FROM silver.bronze_classification;
    SELECT count(*) INTO v_silver_rows FROM silver.measurements;
    SELECT count(*) INTO v_rej_rows FROM silver.rejected_records;
    SELECT count(*) INTO v_unclassified FROM bronze.measurements b
     WHERE NOT EXISTS (SELECT 1 FROM silver.bronze_classification c WHERE c.bronze_id = b.bronze_id);
    SELECT count(*) INTO v_dup_bad FROM silver.bronze_classification c
      LEFT JOIN silver.measurements w ON w.bronze_id = c.duplicate_of_bronze_id
     WHERE c.classification = 'DUPLICATE' AND (w.bronze_id IS NULL OR w.bronze_id >= c.bronze_id);
    SELECT count(*) INTO v_key_dupes FROM (
        SELECT 1 FROM silver.measurements GROUP BY sensor_id, parameter_id, period_from_utc HAVING count(*) > 1) k;
    SELECT count(*) INTO v_mismatch FROM silver.measurements m
      JOIN bronze.measurements b USING (bronze_id)
     WHERE m.pm25_ugm3 IS DISTINCT FROM b.value_raw::numeric
        OR m.period_from_utc IS DISTINCT FROM b.period_from_raw::timestamptz
        OR m.period_to_utc   IS DISTINCT FROM b.period_to_raw::timestamptz
        OR m.sensor_id       IS DISTINCT FROM b.sensor_id::bigint;
    SELECT count(*) INTO v_triggers FROM pg_trigger t
      JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = 'bronze' AND t.tgname IN ('trg_sensors_immutable','trg_measurements_immutable')
       AND t.tgenabled = 'O';

    -- Silver -> Gold
    SELECT count(*) FILTER (WHERE is_flatline), count(*) FILTER (WHERE is_high_value)
      INTO v_s_flat, v_s_high FROM silver.measurements;
    SELECT COALESCE(sum(n_readings),0), COALESCE(sum(n_flatline_readings),0), COALESCE(sum(n_high_value_readings),0)
      INTO v_h_n, v_h_flat, v_h_high FROM gold.hourly_sensor_pm25;
    SELECT COALESCE(sum(n_readings),0) INTO v_d_n FROM gold.daily_location_summary;
    SELECT COALESCE(sum(received_readings),0), COALESCE(sum(n_flatline_readings),0),
           COALESCE(sum(n_high_value_readings),0), count(*)
      INTO v_e_n, v_e_flat, v_e_high, v_health_rows FROM gold.sensor_health;
    SELECT COALESCE(sum(((v_h1 - interval '1 microsecond') AT TIME ZONE s.timezone)::date
                        - (v_h0 AT TIME ZONE s.timezone)::date + 1), 0)
      INTO v_expected_health FROM silver.sensors s WHERE v_h0 IS NOT NULL AND v_h1 IS NOT NULL;
    SELECT (SELECT count(*) FROM gold.hourly_sensor_pm25
             WHERE coverage_pct NOT BETWEEN 0 AND 100 OR covered_seconds NOT BETWEEN 0 AND 3600)
         + (SELECT count(*) FROM gold.daily_location_summary WHERE coverage_pct NOT BETWEEN 0 AND 100)
         + (SELECT count(*) FROM gold.sensor_health WHERE coverage_pct NOT BETWEEN 0 AND 100)
      INTO v_cov_bad;
    SELECT count(*) INTO v_zero_hours FROM gold.hourly_sensor_pm25 WHERE n_readings < 1;

    -- run attribution: the SAME run_id must have flowed through every layer
    SELECT count(*) INTO v_b_run FROM bronze.measurements WHERE run_id = p_run_id;
    SELECT count(*) INTO v_s_run FROM silver.bronze_classification WHERE silver_run_id = p_run_id;
    SELECT (SELECT count(*) FROM gold.hourly_sensor_pm25 WHERE gold_run_id = p_run_id)
         + (SELECT count(*) FROM gold.daily_location_summary WHERE gold_run_id = p_run_id)
         + (SELECT count(*) FROM gold.sensor_health WHERE gold_run_id = p_run_id)
      INTO v_g_run;
    SELECT count(*) INTO v_status_rows FROM audit.sensor_fetch_status WHERE run_id = p_run_id;
    v_window_ok := (v_w0 < v_w1 AND v_h0 IS NOT NULL AND v_h1 IS NOT NULL AND v_h0 <= v_w0 AND v_h1 >= v_w1)::int;

    RETURN QUERY
    SELECT c.n, c.sev, c.e::numeric, c.a::numeric,
           CASE WHEN c.e IS NOT DISTINCT FROM c.a THEN 'PASS' ELSE 'FAIL' END, c.d
    FROM (VALUES
        ('bronze_equals_classified',                  'CRITICAL', v_bronze,          v_class,        'bronze.measurements vs silver.bronze_classification'),
        ('bronze_equals_valid_rejected_duplicate',    'CRITICAL', v_bronze,          v_valid + v_rej + v_dup,
              'VALID=' || v_valid || ' REJECTED=' || v_rej || ' DUPLICATE=' || v_dup),
        ('bronze_rows_without_verdict',               'CRITICAL', 0::bigint,         v_unclassified, 'Bronze rows with no classification'),
        ('silver_measurements_equal_valid',           'CRITICAL', v_valid,           v_silver_rows,  NULL),
        ('rejected_records_equal_rejected',           'CRITICAL', v_rej,             v_rej_rows,     NULL),
        ('duplicates_point_at_earlier_valid_row',     'CRITICAL', 0::bigint,         v_dup_bad,      'DUPLICATE rows whose winner is missing or not earlier'),
        ('silver_natural_key_unique',                 'CRITICAL', 0::bigint,         v_key_dupes,    'keys (sensor, parameter, period_from) with >1 Silver row'),
        ('silver_values_match_bronze',                'CRITICAL', 0::bigint,         v_mismatch,     'Silver typed values that differ from their Bronze source'),
        ('bronze_immutability_triggers_enabled',      'CRITICAL', 2::bigint,         v_triggers,     'UPDATE/DELETE-blocking triggers on both Bronze tables'),
        ('gold_hourly_equals_silver_readings',        'CRITICAL', v_silver_rows,     v_h_n,          NULL),
        ('gold_daily_equals_silver_readings',         'CRITICAL', v_silver_rows,     v_d_n,          NULL),
        ('gold_health_equals_silver_readings',        'CRITICAL', v_silver_rows,     v_e_n,          NULL),
        ('gold_flatline_flags_reconcile',             'CRITICAL', v_s_flat,          v_h_flat,       'hourly vs Silver'),
        ('gold_high_value_flags_reconcile',           'CRITICAL', v_s_high,          v_h_high,       'hourly vs Silver'),
        ('gold_health_flag_counts_reconcile',         'CRITICAL', v_s_flat + v_s_high, v_e_flat + v_e_high, 'health vs Silver'),
        ('gold_health_rows_complete',                 'CRITICAL', v_expected_health, v_health_rows,  'sensors x local days in the effective horizon'),
        ('gold_coverage_within_0_100',                'CRITICAL', 0::bigint,         v_cov_bad,      'rows with coverage outside 0..100'),
        ('gold_hourly_rows_have_readings',            'CRITICAL', 0::bigint,         v_zero_hours,   'hourly rows with n_readings < 1'),
        ('run_window_valid_and_covered',              'CRITICAL', 1::bigint,         v_window_ok::bigint, 'requested window is ordered and inside the effective horizon'),
        ('run_sensors_accounted_for',                 'CRITICAL', v_requested,       v_status_rows,  'discovered sensors vs sensor_fetch_status rows'),
        ('run_bronze_fetched_equals_inserted_plus_skipped', 'CRITICAL', v_fetched,   v_inserted + v_skipped, NULL),
        ('run_bronze_rows_carry_run_id',              'CRITICAL', v_inserted,        v_b_run,        'bronze.measurements.run_id = this run'),
        ('run_silver_rows_carry_run_id',              'CRITICAL', v_classified,      v_s_run,        'silver.bronze_classification.silver_run_id = this run'),
        ('run_gold_rows_carry_run_id',                'CRITICAL', v_gold_written,    v_g_run,        'gold.*.gold_run_id = this run')
    ) AS c(n, sev, e, a, d);

    -- NOTE: output columns (status, severity, details...) are PL/pgSQL variables, so table columns are qualified.
    SELECT string_agg(f.sensor_id::text || ':' || COALESCE(f.last_http_status::text, f.error_type, '?'), ', ' ORDER BY f.sensor_id)
      INTO v_failed_list
      FROM (SELECT sfs.sensor_id, sfs.last_http_status, sfs.error_type FROM audit.sensor_fetch_status sfs
             WHERE sfs.run_id = p_run_id AND sfs.status = 'FAILED' ORDER BY sfs.sensor_id LIMIT 20) f;

    -- Fetch failures are RECORDED, and fail the run only above a tolerance (default 5% of attempted).
    RETURN QUERY SELECT 'run_fetch_failure_rate'::text, 'CRITICAL'::text, 0::numeric, COALESCE(v_failed, 0)::numeric,
        CASE WHEN COALESCE(v_failed, 0) = 0 THEN 'PASS'
             WHEN COALESCE(v_attempted, 0) > 0 AND v_failed::numeric / v_attempted <= v_max_frac THEN 'WARN'
             ELSE 'FAIL' END::text,
        format('%s of %s attempted sensors failed (tolerance %s%%)%s', COALESCE(v_failed,0), COALESCE(v_attempted,0),
               round(v_max_frac * 100, 2), COALESCE('; first: ' || v_failed_list, ''))::text;
END
$$;

-- ---------------------------------------------------------------- views: why is there no data?

-- Each sensor's most recent ATTEMPTED outcome from a SUCCESSFUL run (an informed skip counts as attempted;
-- a later partial run that never reached the sensor does not erase what an earlier run learned).
CREATE OR REPLACE VIEW audit.v_latest_sensor_fetch AS
SELECT DISTINCT ON (f.sensor_id)
       f.sensor_id, f.run_id, f.provider_name, f.status, f.reason, f.rows_fetched,
       f.first_period_from_utc, f.last_period_to_utc, f.last_http_status, f.error_type,
       r.window_from_utc, r.window_to_utc, r.finished_at AS run_finished_at
FROM audit.sensor_fetch_status f
JOIN audit.pipeline_runs r USING (run_id)
WHERE r.status = 'SUCCESS'
ORDER BY f.sensor_id, (f.status <> 'NOT_ATTEMPTED') DESC, r.finished_at DESC;

-- Per run and provider: how many sensors delivered, and how stale is the provider's newest data relative
-- to the requested window end. A large lag with many sensors is source/provider evidence, not sensor failure.
CREATE OR REPLACE VIEW audit.v_provider_freshness AS
SELECT f.run_id, f.provider_name,
       count(*)                                            AS sensors,
       count(*) FILTER (WHERE f.status = 'FETCHED')        AS fetched_with_data,
       count(*) FILTER (WHERE f.status = 'ZERO_ROWS')      AS zero_rows,
       count(*) FILTER (WHERE f.status = 'FAILED')         AS failed,
       count(*) FILTER (WHERE f.status = 'SKIPPED_INACTIVE') AS skipped_inactive,
       count(*) FILTER (WHERE f.status = 'NOT_ATTEMPTED')  AS not_attempted,
       min(f.first_period_from_utc)                        AS first_data_utc,
       max(f.last_period_to_utc)                           AS last_data_utc,
       r.window_to_utc,
       round(EXTRACT(EPOCH FROM (r.window_to_utc - max(f.last_period_to_utc))) / 3600.0, 1) AS hours_behind_window_end
FROM audit.sensor_fetch_status f
JOIN audit.pipeline_runs r USING (run_id)
GROUP BY f.run_id, f.provider_name, r.window_to_utc;

-- Every Gold NO_DATA sensor-day with the most specific reason the audit trail supports:
--   FETCH_FAILED                  request failed (HTTP 5xx, timeout, ...)      -> B
--   SOURCE_RETURNED_ZERO_ROWS     request succeeded, source had nothing         -> A
--   NOT_ATTEMPTED                 we never asked                                -> C
--   SKIPPED_INACTIVE              skipped: source says inactive in the window   -> D
--   PROVIDER_STOPPED_DELIVERING   the day is after the newest data of >= 2 sensors of the provider -> E
--   SENSOR_STOPPED_DELIVERING     the day is after this sensor's newest data
--   GAP_WITHIN_SENSOR_DATA        the sensor has data before and after this day
CREATE OR REPLACE VIEW audit.v_no_data_reason AS
WITH provider_last AS (
    SELECT provider_name, max(last_period_to_utc) AS provider_last, count(*) AS provider_sensors_with_data
    FROM audit.v_latest_sensor_fetch WHERE status = 'FETCHED' GROUP BY provider_name
)
SELECT h.sensor_id, h.local_date, s.provider_name,
       l.status AS fetch_status, l.run_id AS fetch_run_id, l.last_http_status, l.error_type, l.last_period_to_utc,
       CASE
           WHEN l.sensor_id IS NULL                  THEN 'NO_FETCH_RECORD'
           WHEN l.status = 'FAILED'                  THEN 'FETCH_FAILED'
           WHEN l.status = 'NOT_ATTEMPTED'           THEN 'NOT_ATTEMPTED'
           WHEN l.status = 'SKIPPED_INACTIVE'        THEN 'SKIPPED_INACTIVE'
           WHEN l.status = 'ZERO_ROWS'               THEN 'SOURCE_RETURNED_ZERO_ROWS'
           WHEN (h.local_date::timestamp AT TIME ZONE h.timezone) > p.provider_last
                AND p.provider_sensors_with_data >= 2 THEN 'PROVIDER_STOPPED_DELIVERING'
           WHEN (h.local_date::timestamp AT TIME ZONE h.timezone) > l.last_period_to_utc
                                                      THEN 'SENSOR_STOPPED_DELIVERING'
           ELSE 'GAP_WITHIN_SENSOR_DATA'
       END AS no_data_reason
FROM gold.sensor_health h
JOIN silver.sensors s USING (sensor_id)
LEFT JOIN audit.v_latest_sensor_fetch l ON l.sensor_id = h.sensor_id
LEFT JOIN provider_last p ON p.provider_name = s.provider_name
WHERE h.health_status = 'NO_DATA';
