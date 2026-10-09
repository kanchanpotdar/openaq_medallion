-- 02_silver.sql : typed, validated, deduplicated data + per-row classification.
-- Idempotent DDL. All transformation logic is set-based SQL inside silver.run_silver().
--
-- Every Bronze row receives exactly ONE verdict in silver.bronze_classification:
--   VALID      -> row in silver.measurements
--   REJECTED   -> row in silver.rejected_records
--   DUPLICATE  -> pointer to the VALID row that owns the same key
-- Invariant (enforced at the end of every run):
--   count(bronze.measurements) = VALID + REJECTED + DUPLICATE

-- ---------------------------------------------------------------- tables

-- Dimension: latest Bronze snapshot per sensor, properly typed.
CREATE TABLE IF NOT EXISTS silver.sensors (
    sensor_id        BIGINT   PRIMARY KEY,
    location_id      BIGINT   NOT NULL,
    location_name    TEXT,
    provider_name    TEXT,
    timezone         TEXT,
    latitude         NUMERIC  CHECK (latitude  BETWEEN -90  AND 90),
    longitude        NUMERIC  CHECK (longitude BETWEEN -180 AND 180),
    is_monitor       BOOLEAN,
    parameter_id     SMALLINT NOT NULL CHECK (parameter_id = 2),
    source_bronze_id BIGINT   NOT NULL REFERENCES bronze.sensors (bronze_id)
);

-- VALID rows only. ts_utc (canonical event time) == period_from_utc.
CREATE TABLE IF NOT EXISTS silver.measurements (
    bronze_id           BIGINT      PRIMARY KEY REFERENCES bronze.measurements (bronze_id),
    sensor_id           BIGINT      NOT NULL REFERENCES silver.sensors (sensor_id),
    parameter_id        SMALLINT    NOT NULL CHECK (parameter_id = 2),
    period_from_utc     TIMESTAMPTZ NOT NULL,
    period_to_utc       TIMESTAMPTZ NOT NULL,
    duration_seconds    INTEGER GENERATED ALWAYS AS
                        (EXTRACT(EPOCH FROM (period_to_utc - period_from_utc))::integer) STORED,
    pm25_ugm3           NUMERIC     NOT NULL CHECK (pm25_ugm3 >= 0),
    -- soft flags: annotate, never change the verdict
    is_high_value       BOOLEAN     NOT NULL DEFAULT false,
    is_long_gap         BOOLEAN     NOT NULL DEFAULT false,
    is_flatline         BOOLEAN     NOT NULL DEFAULT false,
    gap_before_seconds  BIGINT,     -- this start minus previous end, same sensor
    flatline_run_length INTEGER,    -- only set when is_flatline
    silver_run_id       UUID        NOT NULL,
    loaded_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_silver_meas_period CHECK (period_to_utc > period_from_utc),
    CONSTRAINT uq_silver_measurements_key UNIQUE (sensor_id, parameter_id, period_from_utc)
);
CREATE INDEX IF NOT EXISTS ix_silver_measurements_from ON silver.measurements (period_from_utc);

CREATE TABLE IF NOT EXISTS silver.rejected_records (
    rejected_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    bronze_id        BIGINT      NOT NULL UNIQUE REFERENCES bronze.measurements (bronze_id),
    reason_code      TEXT        NOT NULL CHECK (reason_code IN (
                        'NULL_SENSOR','MISSING_VALUE','UNPARSEABLE_VALUE','UNPARSEABLE_TS',
                        'NEGATIVE_PM25','FUTURE_TIMESTAMP','ORPHAN_SENSOR',
                        'WRONG_PARAMETER','UNEXPECTED_UNIT','INVALID_PERIOD')),
    all_reason_codes TEXT[]      NOT NULL,   -- every rule that fired; reason_code is the first
    reason_detail    TEXT,                   -- the offending raw value(s)
    silver_run_id    UUID        NOT NULL,
    rejected_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS silver.bronze_classification (
    bronze_id              BIGINT      PRIMARY KEY REFERENCES bronze.measurements (bronze_id),
    classification         TEXT        NOT NULL CHECK (classification IN ('VALID','REJECTED','DUPLICATE')),
    reason_code            TEXT,
    duplicate_of_bronze_id BIGINT      REFERENCES silver.measurements (bronze_id),
    value_differs          BOOLEAN,    -- duplicates only: did this copy carry a different value?
    silver_run_id          UUID        NOT NULL,
    classified_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_class_reason    CHECK ((classification = 'REJECTED')  = (reason_code IS NOT NULL)),
    CONSTRAINT ck_class_duplicate CHECK ((classification = 'DUPLICATE') = (duplicate_of_bronze_id IS NOT NULL)),
    CONSTRAINT ck_class_differs   CHECK ((classification = 'DUPLICATE') = (value_differs IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS ix_bronze_class_run ON silver.bronze_classification (silver_run_id);

-- ---------------------------------------------------------------- the transformation

CREATE OR REPLACE FUNCTION silver.run_silver(
    p_run_id            uuid,
    p_full_refresh      boolean  DEFAULT false,
    p_high_value        numeric  DEFAULT 500,                 -- HIGH_VALUE: pm25 >  500
    p_long_gap          interval DEFAULT interval '6 hours',  -- LONG_GAP:   gap  >  6 h
    p_flat_min_readings integer  DEFAULT 8,                   -- FLATLINE:   >= 8 identical readings
    p_flat_min_span     interval DEFAULT interval '4 hours',  --   AND span   >= 4 h
    p_future_tolerance  interval DEFAULT interval '0'         -- FUTURE_TIMESTAMP slack
)
RETURNS TABLE (
    out_classified      bigint, out_valid       bigint, out_rejected    bigint, out_duplicate bigint,
    out_sensors_upserted bigint, out_flag_rows_updated bigint,
    out_bronze_total    bigint, out_valid_total bigint, out_rejected_total bigint, out_duplicate_total bigint
)
LANGUAGE plpgsql AS $$
DECLARE
    -- U+00B5 (micro sign), NOT U+03BC (Greek mu): the two look identical. Phase 3 design check.
    v_unit_ok      constant text := chr(181) || 'g/m' || chr(179);
    v_classified   bigint; v_valid bigint; v_rejected bigint; v_duplicate bigint;
    v_sensors      bigint; v_flags bigint;
    v_bronze_total bigint; v_class_total bigint;
    v_valid_total  bigint; v_rej_total bigint; v_dup_total bigint;
    v_silver_total bigint; v_rejrec_total bigint;
BEGIN
    -- One Silver run at a time.
    PERFORM pg_advisory_xact_lock(hashtext('silver.run_silver'));

    IF p_full_refresh THEN
        -- Rules changed? Throw Silver away and re-derive everything from immutable Bronze.
        TRUNCATE silver.bronze_classification, silver.rejected_records,
                 silver.measurements, silver.sensors;
    END IF;

    ------------------------------------------------------------------ 1. sensor dimension
    -- Latest Bronze snapshot per sensor wins. Shape-check ids BEFORE casting.
    INSERT INTO silver.sensors (sensor_id, location_id, location_name, provider_name, timezone,
                                latitude, longitude, is_monitor, parameter_id, source_bronze_id)
    SELECT s.sensor_id::bigint, s.location_id::bigint, s.location_name, s.provider_name, s.timezone,
           CASE WHEN s.latitude  ~ '^[+-]?[0-9]{1,3}([.][0-9]+)?$'
                THEN CASE WHEN s.latitude::numeric  BETWEEN -90  AND 90  THEN s.latitude::numeric  END END,
           CASE WHEN s.longitude ~ '^[+-]?[0-9]{1,3}([.][0-9]+)?$'
                THEN CASE WHEN s.longitude::numeric BETWEEN -180 AND 180 THEN s.longitude::numeric END END,
           lower(s.is_monitor) = 'true', s.parameter_id::smallint, s.bronze_id
    FROM (
        SELECT DISTINCT ON (b.sensor_id) b.*
        FROM bronze.sensors b
        WHERE b.sensor_id    ~ '^[0-9]{1,18}$'
          AND b.location_id  ~ '^[0-9]{1,18}$'
          AND b.parameter_id = '2'
        ORDER BY b.sensor_id, b.bronze_id DESC
    ) s
    ON CONFLICT (sensor_id) DO UPDATE SET
        location_id = EXCLUDED.location_id, location_name = EXCLUDED.location_name,
        provider_name = EXCLUDED.provider_name, timezone = EXCLUDED.timezone,
        latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude,
        is_monitor = EXCLUDED.is_monitor, source_bronze_id = EXCLUDED.source_bronze_id
    WHERE silver.sensors.source_bronze_id < EXCLUDED.source_bronze_id;
    GET DIAGNOSTICS v_sensors = ROW_COUNT;

    ------------------------------------------------------------------ 2. classify new Bronze rows
    DROP TABLE IF EXISTS _silver_batch;
    CREATE TEMP TABLE _silver_batch ON COMMIT DROP AS
    WITH
    -- A. Work queue: Bronze rows with no verdict yet. NOT EXISTS, never a bronze_id high-water
    --    mark (identity values are not guaranteed to commit in order).
    new_rows AS (
        SELECT m.*
        FROM bronze.measurements m
        WHERE NOT EXISTS (SELECT 1 FROM silver.bronze_classification c WHERE c.bronze_id = m.bronze_id)
    ),
    -- B. Safe parsing. Rule: shape-check with a strict regex, THEN cast, inside CASE (which
    --    guarantees evaluation order). pg_input_is_valid alone is too lenient: it accepts
    --    'now', 'NaN', 'Infinity' and '1e3'.
    parsed AS (
        SELECT n.bronze_id, n.ingested_at,
               n.sensor_id AS sensor_raw, n.parameter_id AS parameter_raw, n.value_raw, n.unit_raw,
               n.period_from_raw, n.period_to_raw,
               CASE WHEN n.sensor_id    ~ '^[0-9]{1,18}$' THEN n.sensor_id::bigint     END AS sensor_num,
               CASE WHEN n.parameter_id ~ '^[0-9]{1,4}$'  THEN n.parameter_id::smallint END AS parameter_num,
               CASE WHEN n.value_raw ~ '^[+-]?[0-9]{1,10}([.][0-9]{1,6})?$'
                    THEN n.value_raw::numeric END AS pm25,
               CASE WHEN n.period_from_raw ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$'
                     AND pg_input_is_valid(n.period_from_raw, 'timestamptz')
                    THEN n.period_from_raw::timestamptz END AS ts_from,
               CASE WHEN n.period_to_raw ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$'
                     AND pg_input_is_valid(n.period_to_raw, 'timestamptz')
                    THEN n.period_to_raw::timestamptz END AS ts_to
        FROM new_rows n
    ),
    -- C. Hard rules. Each CASE yields its code or NULL; ARRAY_REMOVE drops the NULLs.
    --    Array order IS the priority order: element [1] becomes the primary reason_code.
    --    FUTURE_TIMESTAMP compares to ingested_at, not now(), so reruns are deterministic.
    checked AS (
        SELECT p.*,
               ARRAY_REMOVE(ARRAY[
                   CASE WHEN p.sensor_raw IS NULL OR btrim(p.sensor_raw) = '' THEN 'NULL_SENSOR' END,
                   CASE WHEN p.value_raw  IS NULL OR btrim(p.value_raw)  = '' THEN 'MISSING_VALUE' END,
                   CASE WHEN p.value_raw IS NOT NULL AND btrim(p.value_raw) <> '' AND p.pm25 IS NULL
                        THEN 'UNPARSEABLE_VALUE' END,
                   CASE WHEN p.ts_from IS NULL OR p.ts_to IS NULL THEN 'UNPARSEABLE_TS' END,
                   CASE WHEN p.pm25 < 0 THEN 'NEGATIVE_PM25' END,
                   CASE WHEN p.ts_from > p.ingested_at + p_future_tolerance THEN 'FUTURE_TIMESTAMP' END,
                   CASE WHEN p.sensor_raw IS NOT NULL AND btrim(p.sensor_raw) <> ''
                         AND ss.sensor_id IS NULL THEN 'ORPHAN_SENSOR' END,
                   CASE WHEN p.parameter_num IS DISTINCT FROM 2 THEN 'WRONG_PARAMETER' END,
                   CASE WHEN p.unit_raw IS DISTINCT FROM v_unit_ok THEN 'UNEXPECTED_UNIT' END,
                   CASE WHEN p.ts_from IS NOT NULL AND p.ts_to IS NOT NULL AND p.ts_to <= p.ts_from
                        THEN 'INVALID_PERIOD' END
               ]::text[], NULL) AS reasons
        FROM parsed p
        LEFT JOIN silver.sensors ss ON ss.sensor_id = p.sensor_num
    ),
    -- D. Dedupe, applied ONLY to rows that passed every hard rule (a rejected row must never
    --    claim a key). Winner = lowest bronze_id, or the row already sitting in Silver.
    survivors AS (
        SELECT c.*,
               e.bronze_id AS existing_id, e.pm25_ugm3 AS existing_pm25,
               row_number()  OVER w AS rn,
               first_value(c.bronze_id) OVER w AS batch_first_id,
               first_value(c.pm25)      OVER w AS batch_first_pm25
        FROM checked c
        LEFT JOIN silver.measurements e
               ON e.sensor_id = c.sensor_num AND e.parameter_id = c.parameter_num
              AND e.period_from_utc = c.ts_from
        WHERE cardinality(c.reasons) = 0
        WINDOW w AS (PARTITION BY c.sensor_num, c.parameter_num, c.ts_from ORDER BY c.bronze_id)
    )
    -- E. One verdict row per input row.
    SELECT s.bronze_id, s.sensor_num, s.parameter_num, s.ts_from, s.ts_to, s.pm25,
           CASE WHEN s.existing_id IS NOT NULL OR s.rn > 1 THEN 'DUPLICATE' ELSE 'VALID' END AS classification,
           NULL::text   AS reason_code,
           NULL::text[] AS all_reasons,
           NULL::text   AS reason_detail,
           CASE WHEN s.existing_id IS NOT NULL THEN s.existing_id
                WHEN s.rn > 1 THEN s.batch_first_id END AS duplicate_of,
           CASE WHEN s.existing_id IS NOT NULL THEN s.pm25 IS DISTINCT FROM s.existing_pm25
                WHEN s.rn > 1 THEN s.pm25 IS DISTINCT FROM s.batch_first_pm25 END AS value_differs
    FROM survivors s
    UNION ALL
    SELECT c.bronze_id, c.sensor_num, c.parameter_num, c.ts_from, c.ts_to, c.pm25,
           'REJECTED', c.reasons[1], c.reasons,
           CASE c.reasons[1]
               WHEN 'NULL_SENSOR'       THEN 'sensor_id=' || COALESCE(quote_literal(c.sensor_raw), 'NULL')
               WHEN 'MISSING_VALUE'     THEN 'value=' || COALESCE(quote_literal(c.value_raw), 'NULL')
               WHEN 'UNPARSEABLE_VALUE' THEN 'value=' || quote_literal(c.value_raw)
               WHEN 'UNPARSEABLE_TS'    THEN 'from=' || COALESCE(quote_literal(c.period_from_raw), 'NULL')
                                          || ' to='   || COALESCE(quote_literal(c.period_to_raw), 'NULL')
               WHEN 'NEGATIVE_PM25'     THEN 'value=' || quote_literal(c.value_raw)
               WHEN 'FUTURE_TIMESTAMP'  THEN 'from=' || quote_literal(c.period_from_raw)
                                          || ' ingested_at=' || c.ingested_at::text
               WHEN 'ORPHAN_SENSOR'     THEN 'sensor_id=' || quote_literal(c.sensor_raw)
               WHEN 'WRONG_PARAMETER'   THEN 'parameter_id=' || COALESCE(quote_literal(c.parameter_raw), 'NULL')
               WHEN 'UNEXPECTED_UNIT'   THEN 'unit=' || COALESCE(quote_literal(c.unit_raw), 'NULL')
               WHEN 'INVALID_PERIOD'    THEN 'from=' || quote_literal(c.period_from_raw)
                                          || ' to='   || quote_literal(c.period_to_raw)
           END,
           NULL::bigint, NULL::boolean
    FROM checked c
    WHERE cardinality(c.reasons) > 0;

    SELECT count(*),
           count(*) FILTER (WHERE classification = 'VALID'),
           count(*) FILTER (WHERE classification = 'REJECTED'),
           count(*) FILTER (WHERE classification = 'DUPLICATE')
      INTO v_classified, v_valid, v_rejected, v_duplicate
      FROM _silver_batch;

    -- Write order matters: classification points at silver.measurements (FK), so VALID first.
    INSERT INTO silver.measurements (bronze_id, sensor_id, parameter_id, period_from_utc,
                                     period_to_utc, pm25_ugm3, silver_run_id)
    SELECT bronze_id, sensor_num, parameter_num, ts_from, ts_to, pm25, p_run_id
    FROM _silver_batch WHERE classification = 'VALID';

    INSERT INTO silver.rejected_records (bronze_id, reason_code, all_reason_codes, reason_detail, silver_run_id)
    SELECT bronze_id, reason_code, all_reasons, reason_detail, p_run_id
    FROM _silver_batch WHERE classification = 'REJECTED';

    INSERT INTO silver.bronze_classification (bronze_id, classification, reason_code,
                                              duplicate_of_bronze_id, value_differs, silver_run_id)
    SELECT bronze_id, classification, reason_code, duplicate_of, value_differs, p_run_id
    FROM _silver_batch;

    ------------------------------------------------------------------ 3. soft flags
    -- Flags depend on neighbouring readings, so they are RECOMPUTED for every sensor that
    -- received new VALID rows (a late reading can close an old gap).
    WITH touched AS (
        SELECT DISTINCT sensor_num AS sensor_id FROM _silver_batch WHERE classification = 'VALID'
    ),
    -- a. each reading beside its predecessor (same sensor, ordered by start)
    s AS (
        SELECT m.bronze_id, m.sensor_id, m.period_from_utc AS f, m.period_to_utc AS t,
               m.pm25_ugm3 AS v, m.duration_seconds AS dur,
               lag(m.period_to_utc) OVER w AS prev_to,
               lag(m.pm25_ugm3)     OVER w AS prev_v
        FROM silver.measurements m
        JOIN touched USING (sensor_id)
        WINDOW w AS (PARTITION BY m.sensor_id ORDER BY m.period_from_utc)
    ),
    g AS (
        SELECT s.*, EXTRACT(EPOCH FROM (f - prev_to))::bigint AS gap_sec FROM s
    ),
    -- b. gaps-and-islands: a new run starts when the value changes OR the gap before it is
    --    more than 2x the reading's own interval (identical values across an outage are not
    --    one continuous flatline).
    r AS (
        SELECT g.*,
               SUM(CASE WHEN prev_v IS DISTINCT FROM v OR gap_sec > 2 * dur THEN 1 ELSE 0 END)
                   OVER (PARTITION BY sensor_id ORDER BY f
                         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS run_no
        FROM g
    ),
    -- c. run size and span (first start -> last end)
    agg AS (
        SELECT r.*,
               count(*)            OVER (PARTITION BY sensor_id, run_no) AS run_len,
               max(t) OVER (PARTITION BY sensor_id, run_no)
             - min(f) OVER (PARTITION BY sensor_id, run_no)              AS run_span
        FROM r
    ),
    flags AS (
        SELECT a.bronze_id, a.gap_sec,
               COALESCE(a.gap_sec > EXTRACT(EPOCH FROM p_long_gap), false)           AS long_gap,
               (a.v > p_high_value)                                                  AS high_value,
               (a.run_len >= p_flat_min_readings AND a.run_span >= p_flat_min_span)  AS flat,
               a.run_len
        FROM agg a
    )
    UPDATE silver.measurements m
       SET gap_before_seconds  = f.gap_sec,
           is_long_gap         = f.long_gap,
           is_high_value       = f.high_value,
           is_flatline         = f.flat,
           flatline_run_length = CASE WHEN f.flat THEN f.run_len END
      FROM flags f
     WHERE m.bronze_id = f.bronze_id
       AND (m.gap_before_seconds, m.is_long_gap, m.is_high_value, m.is_flatline, m.flatline_run_length)
           IS DISTINCT FROM
           (f.gap_sec, f.long_gap, f.high_value, f.flat, CASE WHEN f.flat THEN f.run_len END);
    GET DIAGNOSTICS v_flags = ROW_COUNT;

    ------------------------------------------------------------------ 4. reconciliation guard
    -- If this fails the exception rolls back the WHOLE run (one transaction).
    SELECT count(*) INTO v_bronze_total FROM bronze.measurements;
    SELECT count(*),
           count(*) FILTER (WHERE classification = 'VALID'),
           count(*) FILTER (WHERE classification = 'REJECTED'),
           count(*) FILTER (WHERE classification = 'DUPLICATE')
      INTO v_class_total, v_valid_total, v_rej_total, v_dup_total
      FROM silver.bronze_classification;
    SELECT count(*) INTO v_silver_total  FROM silver.measurements;
    SELECT count(*) INTO v_rejrec_total  FROM silver.rejected_records;

    IF v_bronze_total <> v_valid_total + v_rej_total + v_dup_total
       OR v_class_total  <> v_bronze_total
       OR v_silver_total <> v_valid_total
       OR v_rejrec_total <> v_rej_total THEN
        RAISE EXCEPTION 'silver reconciliation failed: bronze=% classified=% (valid=% rejected=% duplicate=%) silver_rows=% rejected_rows=%',
            v_bronze_total, v_class_total, v_valid_total, v_rej_total, v_dup_total,
            v_silver_total, v_rejrec_total;
    END IF;

    RETURN QUERY SELECT v_classified, v_valid, v_rejected, v_duplicate, v_sensors, v_flags,
                        v_bronze_total, v_valid_total, v_rej_total, v_dup_total;
END
$$;
