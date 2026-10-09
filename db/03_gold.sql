-- 03_gold.sql : analytics tables built ONLY from Silver. Idempotent DDL.
--
-- NO foreign keys to Silver on purpose: silver.run_silver(full_refresh) TRUNCATEs silver.sensors,
-- and Postgres refuses to truncate a table another table references. Integrity is enforced by
-- the reconciliation guards (G1-G6) at the end of gold.run_gold() instead.
--
-- Attribution rule (approved): a reading belongs to the LOCAL hour / LOCAL date in which its
-- period_from_utc falls. UTC-aligned hourly sensors (local :30) therefore smear by up to 30 min.

-- ---------------------------------------------------------------- tables

CREATE TABLE IF NOT EXISTS gold.hourly_sensor_pm25 (
    sensor_id                BIGINT        NOT NULL,
    hour_start_utc           TIMESTAMPTZ   NOT NULL,   -- instant of the local hour start (:30 UTC for IST)
    local_hour_start         TIMESTAMP     NOT NULL,
    local_date               DATE          NOT NULL,
    location_id              BIGINT        NOT NULL,
    timezone                 TEXT          NOT NULL,
    avg_pm25                 NUMERIC(10,3) NOT NULL,   -- duration-weighted, ALL valid readings
    avg_pm25_excl_flatline   NUMERIC(10,3),            -- NULL when every reading is flatline-flagged
    min_pm25                 NUMERIC       NOT NULL,
    max_pm25                 NUMERIC       NOT NULL,
    n_readings               INTEGER       NOT NULL,
    reading_interval_seconds INTEGER       NOT NULL,   -- modal duration within the hour
    covered_seconds          INTEGER       NOT NULL,
    coverage_pct             NUMERIC(5,2)  NOT NULL,
    n_flatline_readings      INTEGER       NOT NULL,
    n_high_value_readings    INTEGER       NOT NULL,
    gold_run_id              UUID          NOT NULL,   -- last run that CHANGED this row
    built_at                 TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT pk_gold_hourly PRIMARY KEY (sensor_id, hour_start_utc),
    CONSTRAINT ck_hourly_n         CHECK (n_readings >= 1),
    CONSTRAINT ck_hourly_covered   CHECK (covered_seconds BETWEEN 0 AND 3600),
    CONSTRAINT ck_hourly_coverage  CHECK (coverage_pct BETWEEN 0 AND 100),
    CONSTRAINT ck_hourly_avg       CHECK (avg_pm25 >= 0)
);
CREATE INDEX IF NOT EXISTS ix_gold_hourly_loc_hour ON gold.hourly_sensor_pm25 (location_id, local_hour_start);

CREATE TABLE IF NOT EXISTS gold.daily_location_summary (
    location_id              BIGINT        NOT NULL,
    local_date               DATE          NOT NULL,
    location_name            TEXT,
    timezone                 TEXT          NOT NULL,
    latitude                 NUMERIC,
    longitude                NUMERIC,
    avg_pm25                 NUMERIC(10,3) NOT NULL,   -- mean of location-hour means
    avg_pm25_excl_flatline   NUMERIC(10,3),
    min_hourly_avg           NUMERIC(10,3) NOT NULL,
    max_hourly_avg           NUMERIC(10,3) NOT NULL,
    max_reading              NUMERIC       NOT NULL,   -- extremes stay visible
    n_sensors_reporting      INTEGER       NOT NULL,
    n_sensors_at_location    INTEGER       NOT NULL,
    n_readings               INTEGER       NOT NULL,
    n_location_hours         INTEGER       NOT NULL,
    expected_seconds         INTEGER       NOT NULL,   -- local-day length clipped to the data horizon
    covered_seconds          INTEGER       NOT NULL,
    coverage_pct             NUMERIC(5,2)  NOT NULL,
    is_partial_day           BOOLEAN       NOT NULL,
    meets_completeness       BOOLEAN       NOT NULL,
    n_flatline_readings      INTEGER       NOT NULL,
    n_high_value_readings    INTEGER       NOT NULL,
    gold_run_id              UUID          NOT NULL,
    built_at                 TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT pk_gold_daily PRIMARY KEY (location_id, local_date),
    CONSTRAINT ck_daily_coverage CHECK (coverage_pct BETWEEN 0 AND 100),
    CONSTRAINT ck_daily_hours    CHECK (n_location_hours >= 1)
);

CREATE TABLE IF NOT EXISTS gold.sensor_health (
    sensor_id                 BIGINT       NOT NULL,
    local_date                DATE         NOT NULL,
    location_id               BIGINT       NOT NULL,
    timezone                  TEXT         NOT NULL,
    expected_interval_seconds INTEGER,                 -- NULL if the sensor never reported
    expected_seconds          INTEGER      NOT NULL,   -- local-day length clipped to the horizon
    expected_readings         INTEGER,                 -- ROUND(expected_seconds / interval); never hard-coded
    received_readings         INTEGER      NOT NULL,
    covered_seconds           INTEGER      NOT NULL,
    coverage_pct              NUMERIC(5,2) NOT NULL,
    longest_gap_minutes       NUMERIC(10,1) NOT NULL,
    n_flatline_readings       INTEGER      NOT NULL,
    n_high_value_readings     INTEGER      NOT NULL,
    n_long_gap_readings       INTEGER      NOT NULL,
    flatline_pct              NUMERIC(5,2),
    first_reading_utc         TIMESTAMPTZ,             -- earliest period_from that day
    last_reading_utc          TIMESTAMPTZ,             -- latest period_from that day
    is_partial_day            BOOLEAN      NOT NULL,
    health_status             TEXT         NOT NULL,
    gold_run_id               UUID         NOT NULL,
    built_at                  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT pk_gold_health PRIMARY KEY (sensor_id, local_date),
    CONSTRAINT ck_health_coverage CHECK (coverage_pct BETWEEN 0 AND 100),
    CONSTRAINT ck_health_status CHECK (health_status IN ('NO_DATA','STUCK','LOW_COVERAGE','PARTIAL','OK'))
);
CREATE INDEX IF NOT EXISTS ix_gold_health_date ON gold.sensor_health (local_date, health_status);

-- ---------------------------------------------------------------- the build

CREATE OR REPLACE FUNCTION gold.run_gold(
    p_run_id               uuid,
    p_completeness_pct     numeric     DEFAULT 75,   -- daily meets_completeness: coverage >= 75
    p_stuck_flatline_pct   numeric     DEFAULT 90,   -- STUCK:        flatline_pct >= 90
    p_low_coverage_pct     numeric     DEFAULT 50,   -- LOW_COVERAGE: coverage     <  50
    p_partial_coverage_pct numeric     DEFAULT 90,   -- PARTIAL:      coverage     <  90
    p_horizon_from         timestamptz DEFAULT NULL, -- v1 proxy: derived from Silver. Phase 5 will pass
    p_horizon_to           timestamptz DEFAULT NULL  --   the true ingestion window from audit.pipeline_runs
)
RETURNS TABLE (
    out_hourly_inserted bigint, out_hourly_updated bigint, out_hourly_deleted bigint,
    out_daily_inserted  bigint, out_daily_updated  bigint, out_daily_deleted  bigint,
    out_health_inserted bigint, out_health_updated bigint, out_health_deleted bigint,
    out_hourly_total bigint, out_daily_total bigint, out_health_total bigint,
    out_silver_readings bigint,
    out_horizon_from timestamptz, out_horizon_to timestamptz
)
LANGUAGE plpgsql AS $$
DECLARE
    v_tz   text;
    v_bad  text;
    v_h0   timestamptz;
    v_h1   timestamptz;
    v_min_from timestamptz;
    v_max_to   timestamptz;
    v_silver bigint; v_s_flat bigint; v_s_high bigint;
    v_hi bigint; v_hu bigint; v_hd bigint;
    v_di bigint; v_du bigint; v_dd bigint;
    v_ei bigint; v_eu bigint; v_ed bigint;
    v_h_n bigint; v_h_flat bigint; v_h_high bigint;
    v_d_n bigint; v_d_flat bigint; v_d_high bigint;
    v_e_n bigint; v_e_rows bigint; v_e_expected bigint; v_e_missing bigint;
    v_bad_rows bigint;
    v_h_total bigint; v_d_total bigint; v_e_total bigint;
BEGIN
    -- Serialise with Silver (Gold must never read a half-finished Silver run) and with itself.
    -- Always the same order (silver, then gold) so two lockers cannot deadlock.
    PERFORM pg_advisory_xact_lock(hashtext('silver.run_silver'));
    PERFORM pg_advisory_xact_lock(hashtext('gold.run_gold'));

    ------------------------------------------------------------------ 0. validation
    -- Fail loudly instead of silently dropping sensors (which would break reconciliation).
    FOR v_tz IN SELECT DISTINCT timezone FROM silver.sensors LOOP
        IF v_tz IS NULL OR NOT EXISTS (SELECT 1 FROM pg_timezone_names n WHERE n.name = v_tz) THEN
            SELECT string_agg(sensor_id::text, ',' ORDER BY sensor_id) INTO v_bad
              FROM (SELECT sensor_id FROM silver.sensors
                     WHERE timezone IS NOT DISTINCT FROM v_tz ORDER BY sensor_id LIMIT 20) b;
            RAISE EXCEPTION 'gold: invalid or NULL timezone % for sensors: %', COALESCE(v_tz, '<NULL>'), v_bad;
        END IF;
    END LOOP;

    SELECT string_agg(location_id::text, ',' ORDER BY location_id) INTO v_bad
      FROM (SELECT location_id FROM silver.sensors
             GROUP BY location_id HAVING count(DISTINCT timezone) > 1 LIMIT 20) b;
    IF v_bad IS NOT NULL THEN
        RAISE EXCEPTION 'gold: locations with sensors in different timezones: %', v_bad;
    END IF;

    SELECT min(period_from_utc), max(period_to_utc), count(*),
           count(*) FILTER (WHERE is_flatline), count(*) FILTER (WHERE is_high_value)
      INTO v_min_from, v_max_to, v_silver, v_s_flat, v_s_high
      FROM silver.measurements;

    v_h0 := COALESCE(p_horizon_from, v_min_from);
    v_h1 := COALESCE(p_horizon_to,   v_max_to);
    IF v_silver > 0 AND (v_h0 > v_min_from OR v_h1 < v_max_to) THEN
        RAISE EXCEPTION 'gold: horizon [%, %] does not cover the Silver data [%, %]',
                        v_h0, v_h1, v_min_from, v_max_to;
    END IF;

    ------------------------------------------------------------------ 1. hourly (sensor x local hour)
    -- Duration-weighted mean; coverage in seconds so 5/15/60-minute sensors are comparable.
    CREATE TEMP TABLE _g_hourly ON COMMIT DROP AS
    SELECT x.sensor_id,
           (x.lh AT TIME ZONE x.timezone)                        AS hour_start_utc,
           x.lh                                                  AS local_hour_start,
           x.lh::date                                            AS local_date,
           x.location_id, x.timezone,
           ROUND(SUM(x.v * x.dur) / SUM(x.dur), 3)               AS avg_pm25,
           ROUND(SUM(x.v * x.dur) FILTER (WHERE NOT x.flat)
                 / NULLIF(SUM(x.dur) FILTER (WHERE NOT x.flat), 0), 3) AS avg_pm25_excl_flatline,
           MIN(x.v) AS min_pm25, MAX(x.v) AS max_pm25,
           COUNT(*)::int                                         AS n_readings,
           (mode() WITHIN GROUP (ORDER BY x.dur))::int           AS reading_interval_seconds,
           LEAST(3600, SUM(x.dur))::int                          AS covered_seconds,
           ROUND(100.0 * LEAST(3600, SUM(x.dur)) / 3600, 2)      AS coverage_pct,
           (COUNT(*) FILTER (WHERE x.flat))::int                 AS n_flatline_readings,
           (COUNT(*) FILTER (WHERE x.high))::int                 AS n_high_value_readings
    FROM (
        SELECT m.sensor_id, s.location_id, s.timezone,
               m.pm25_ugm3 AS v, m.duration_seconds AS dur,
               m.is_flatline AS flat, m.is_high_value AS high,
               date_trunc('hour', m.period_from_utc AT TIME ZONE s.timezone) AS lh
        FROM silver.measurements m
        JOIN silver.sensors s USING (sensor_id)
    ) x
    GROUP BY x.sensor_id, x.location_id, x.timezone, x.lh;

    ------------------------------------------------------------------ 2. daily (location x local date)
    CREATE TEMP TABLE _g_daily ON COMMIT DROP AS
    WITH
    -- stage 1: one mean per location-hour, EQUAL weight per reporting sensor (a 15-min sensor must
    -- not outweigh an hourly one); AVG() ignores NULL excl-flatline values.
    lh AS (
        SELECT h.location_id, h.timezone, h.local_hour_start, h.local_date,
               AVG(h.avg_pm25)                AS lavg,
               AVG(h.avg_pm25_excl_flatline)  AS lavg_excl,
               MAX(h.covered_seconds)         AS covered,
               SUM(h.n_readings)              AS n,
               MAX(h.max_pm25)                AS mx,
               SUM(h.n_flatline_readings)     AS nf,
               SUM(h.n_high_value_readings)   AS nh
        FROM _g_hourly h
        GROUP BY h.location_id, h.timezone, h.local_hour_start, h.local_date
    ),
    -- stage 2: each hour with data counts once, however many sensors reported it
    dd AS (
        SELECT lh.location_id, lh.timezone, lh.local_date,
               ROUND(AVG(lh.lavg), 3)       AS avg_pm25,
               ROUND(AVG(lh.lavg_excl), 3)  AS avg_excl,
               ROUND(MIN(lh.lavg), 3)       AS min_h,
               ROUND(MAX(lh.lavg), 3)       AS max_h,
               MAX(lh.mx)                   AS max_reading,
               SUM(lh.n)::int               AS n_readings,
               COUNT(*)::int                AS n_location_hours,
               SUM(lh.covered)::int         AS covered_seconds,
               SUM(lh.nf)::int              AS nf,
               SUM(lh.nh)::int              AS nh
        FROM lh
        GROUP BY lh.location_id, lh.timezone, lh.local_date
    ),
    sens AS (
        SELECT h.location_id, h.local_date, COUNT(DISTINCT h.sensor_id)::int AS n_reporting
        FROM _g_hourly h GROUP BY h.location_id, h.local_date
    ),
    loc AS (  -- location attributes from its lowest sensor_id (verified consistent in Phase 4 design)
        SELECT DISTINCT ON (s.location_id) s.location_id, s.location_name, s.latitude, s.longitude
        FROM silver.sensors s ORDER BY s.location_id, s.sensor_id
    ),
    nloc AS (SELECT s.location_id, COUNT(*)::int AS n FROM silver.sensors s GROUP BY s.location_id),
    x AS (  -- local day boundaries as instants; length comes from timestamps, never "24 h"
        SELECT dd.*,
               (dd.local_date::timestamp       AT TIME ZONE dd.timezone) AS day_start,
               ((dd.local_date + 1)::timestamp AT TIME ZONE dd.timezone) AS day_end
        FROM dd
    ),
    y AS (
        SELECT x.*,
               EXTRACT(EPOCH FROM (LEAST(x.day_end, v_h1) - GREATEST(x.day_start, v_h0)))::int AS expected_seconds,
               EXTRACT(EPOCH FROM (x.day_end - x.day_start))::int                            AS full_day_seconds
        FROM x
    ),
    z AS (
        SELECT y.*,
               ROUND(100.0 * LEAST(y.covered_seconds, y.expected_seconds) / y.expected_seconds, 2) AS coverage_pct
        FROM y
    )
    SELECT z.location_id, z.local_date, loc.location_name, z.timezone, loc.latitude, loc.longitude,
           z.avg_pm25, z.avg_excl AS avg_pm25_excl_flatline,
           z.min_h AS min_hourly_avg, z.max_h AS max_hourly_avg, z.max_reading,
           sens.n_reporting AS n_sensors_reporting, nloc.n AS n_sensors_at_location,
           z.n_readings, z.n_location_hours,
           z.expected_seconds, LEAST(z.covered_seconds, z.expected_seconds) AS covered_seconds,
           z.coverage_pct,
           (z.expected_seconds < z.full_day_seconds) AS is_partial_day,
           (z.coverage_pct >= p_completeness_pct)    AS meets_completeness,
           z.nf AS n_flatline_readings, z.nh AS n_high_value_readings
    FROM z
    JOIN loc  USING (location_id)
    JOIN nloc USING (location_id)
    JOIN sens USING (location_id, local_date);

    ------------------------------------------------------------------ 3. sensor health (ALL sensors x local days)
    CREATE TEMP TABLE _g_health ON COMMIT DROP AS
    WITH
    rng AS (  -- first and last local date the data horizon touches, per sensor timezone
        SELECT s.sensor_id, s.location_id, s.timezone,
               (v_h0 AT TIME ZONE s.timezone)::date                              AS d0,
               ((v_h1 - interval '1 microsecond') AT TIME ZONE s.timezone)::date AS d1
        FROM silver.sensors s
        WHERE v_h0 IS NOT NULL
    ),
    days AS (  -- the spine: every sensor gets a row for every day, whether or not it reported
        SELECT r.sensor_id, r.location_id, r.timezone, r.d0 + g AS local_date
        FROM rng r CROSS JOIN LATERAL generate_series(0, r.d1 - r.d0) g
    ),
    win AS (   -- effective window = local day clipped to the horizon (edge days are partial)
        SELECT d.*,
               (d.local_date::timestamp       AT TIME ZONE d.timezone) AS day_start,
               ((d.local_date + 1)::timestamp AT TIME ZONE d.timezone) AS day_end,
               GREATEST((d.local_date::timestamp       AT TIME ZONE d.timezone), v_h0) AS eff_start,
               LEAST(((d.local_date + 1)::timestamp    AT TIME ZONE d.timezone), v_h1) AS eff_end
        FROM days d
    ),
    rd AS (    -- readings, attributed to the local date in which they START
        SELECT m.sensor_id, (m.period_from_utc AT TIME ZONE s.timezone)::date AS local_date,
               COUNT(*)                                        AS n,
               SUM(m.duration_seconds)                         AS cov,
               mode() WITHIN GROUP (ORDER BY m.duration_seconds) AS day_mode,
               COUNT(*) FILTER (WHERE m.is_flatline)           AS nflat,
               COUNT(*) FILTER (WHERE m.is_high_value)         AS nhigh,
               COUNT(*) FILTER (WHERE m.is_long_gap)           AS nlg,
               MIN(m.period_from_utc)                          AS first_f,
               MAX(m.period_from_utc)                          AS last_f
        FROM silver.measurements m JOIN silver.sensors s USING (sensor_id)
        GROUP BY 1, 2
    ),
    sm AS (SELECT sensor_id, mode() WITHIN GROUP (ORDER BY duration_seconds) AS all_mode
           FROM silver.measurements GROUP BY sensor_id),
    -- Gaps are found on the sensor's WHOLE series, then clipped to each day. Doing it per day would
    -- invent a 30-minute "gap" at the start of the day for UTC-aligned hourly sensors.
    ord AS (
        SELECT m.sensor_id, m.period_from_utc AS f, m.period_to_utc AS t,
               max(m.period_to_utc) OVER (PARTITION BY m.sensor_id ORDER BY m.period_from_utc
                                          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_end
        FROM silver.measurements m
    ),
    gaps AS (
        SELECT sensor_id, COALESCE(prev_end, v_h0) AS g_start, f AS g_end        -- head + internal gaps
        FROM ord WHERE f > COALESCE(prev_end, v_h0)
        UNION ALL
        SELECT sensor_id, max(t), v_h1 FROM ord GROUP BY sensor_id HAVING max(t) < v_h1   -- tail gap
        UNION ALL
        SELECT s.sensor_id, v_h0, v_h1                                            -- never reported
        FROM silver.sensors s
        WHERE v_h0 IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM silver.measurements m WHERE m.sensor_id = s.sensor_id)
    ),
    lg AS (
        SELECT w.sensor_id, w.local_date,
               MAX(LEAST(g.g_end, w.eff_end) - GREATEST(g.g_start, w.eff_start)) AS longest
        FROM win w
        JOIN gaps g ON g.sensor_id = w.sensor_id AND g.g_start < w.eff_end AND g.g_end > w.eff_start
        GROUP BY w.sensor_id, w.local_date
    ),
    h AS (
        SELECT w.sensor_id, w.local_date, w.location_id, w.timezone,
               COALESCE(rd.day_mode, sm.all_mode)::int                           AS expected_interval_seconds,
               EXTRACT(EPOCH FROM (w.eff_end - w.eff_start))::int                AS expected_seconds,
               CASE WHEN COALESCE(rd.day_mode, sm.all_mode) IS NULL THEN NULL
                    ELSE ROUND(EXTRACT(EPOCH FROM (w.eff_end - w.eff_start))
                               / COALESCE(rd.day_mode, sm.all_mode))::int END    AS expected_readings,
               COALESCE(rd.n, 0)::int                                            AS received_readings,
               LEAST(COALESCE(rd.cov, 0), EXTRACT(EPOCH FROM (w.eff_end - w.eff_start)))::int AS covered_seconds,
               ROUND(100.0 * LEAST(COALESCE(rd.cov, 0), EXTRACT(EPOCH FROM (w.eff_end - w.eff_start)))
                     / EXTRACT(EPOCH FROM (w.eff_end - w.eff_start)), 2)         AS coverage_pct,
               ROUND(EXTRACT(EPOCH FROM COALESCE(lg.longest, interval '0')) / 60.0, 1) AS longest_gap_minutes,
               COALESCE(rd.nflat, 0)::int  AS n_flatline_readings,
               COALESCE(rd.nhigh, 0)::int  AS n_high_value_readings,
               COALESCE(rd.nlg, 0)::int    AS n_long_gap_readings,
               CASE WHEN rd.n > 0 THEN ROUND(100.0 * rd.nflat / rd.n, 2) END     AS flatline_pct,
               rd.first_f AS first_reading_utc, rd.last_f AS last_reading_utc,
               ((w.eff_end - w.eff_start) < (w.day_end - w.day_start))           AS is_partial_day
        FROM win w
        LEFT JOIN rd ON rd.sensor_id = w.sensor_id AND rd.local_date = w.local_date
        LEFT JOIN sm ON sm.sensor_id = w.sensor_id
        LEFT JOIN lg ON lg.sensor_id = w.sensor_id AND lg.local_date = w.local_date
    )
    SELECT h.*,
           CASE WHEN h.received_readings = 0                       THEN 'NO_DATA'
                WHEN h.flatline_pct >= p_stuck_flatline_pct        THEN 'STUCK'
                WHEN h.coverage_pct <  p_low_coverage_pct          THEN 'LOW_COVERAGE'
                WHEN h.coverage_pct <  p_partial_coverage_pct      THEN 'PARTIAL'
                ELSE 'OK' END AS health_status
    FROM h;

    ------------------------------------------------------------------ 4. diff-upsert + delete stale
    -- Only rows whose values differ are written, so an unchanged rerun writes 0 rows.
    -- (xmax = 0) distinguishes inserted rows from updated ones.
    WITH up AS (
        INSERT INTO gold.hourly_sensor_pm25 AS t
               (sensor_id, hour_start_utc, local_hour_start, local_date, location_id, timezone,
                avg_pm25, avg_pm25_excl_flatline, min_pm25, max_pm25, n_readings,
                reading_interval_seconds, covered_seconds, coverage_pct,
                n_flatline_readings, n_high_value_readings, gold_run_id)
        SELECT g.sensor_id, g.hour_start_utc, g.local_hour_start, g.local_date, g.location_id, g.timezone,
               g.avg_pm25, g.avg_pm25_excl_flatline, g.min_pm25, g.max_pm25, g.n_readings,
               g.reading_interval_seconds, g.covered_seconds, g.coverage_pct,
               g.n_flatline_readings, g.n_high_value_readings, p_run_id
        FROM _g_hourly g
        ON CONFLICT (sensor_id, hour_start_utc) DO UPDATE SET
               local_hour_start = EXCLUDED.local_hour_start, local_date = EXCLUDED.local_date,
               location_id = EXCLUDED.location_id, timezone = EXCLUDED.timezone,
               avg_pm25 = EXCLUDED.avg_pm25, avg_pm25_excl_flatline = EXCLUDED.avg_pm25_excl_flatline,
               min_pm25 = EXCLUDED.min_pm25, max_pm25 = EXCLUDED.max_pm25,
               n_readings = EXCLUDED.n_readings, reading_interval_seconds = EXCLUDED.reading_interval_seconds,
               covered_seconds = EXCLUDED.covered_seconds, coverage_pct = EXCLUDED.coverage_pct,
               n_flatline_readings = EXCLUDED.n_flatline_readings,
               n_high_value_readings = EXCLUDED.n_high_value_readings,
               gold_run_id = EXCLUDED.gold_run_id, built_at = now()
        WHERE (t.local_hour_start, t.local_date, t.location_id, t.timezone, t.avg_pm25,
               t.avg_pm25_excl_flatline, t.min_pm25, t.max_pm25, t.n_readings,
               t.reading_interval_seconds, t.covered_seconds, t.coverage_pct,
               t.n_flatline_readings, t.n_high_value_readings)
              IS DISTINCT FROM
              (EXCLUDED.local_hour_start, EXCLUDED.local_date, EXCLUDED.location_id, EXCLUDED.timezone,
               EXCLUDED.avg_pm25, EXCLUDED.avg_pm25_excl_flatline, EXCLUDED.min_pm25, EXCLUDED.max_pm25,
               EXCLUDED.n_readings, EXCLUDED.reading_interval_seconds, EXCLUDED.covered_seconds,
               EXCLUDED.coverage_pct, EXCLUDED.n_flatline_readings, EXCLUDED.n_high_value_readings)
        RETURNING (xmax = 0) AS ins
    )
    SELECT count(*) FILTER (WHERE ins), count(*) FILTER (WHERE NOT ins) INTO v_hi, v_hu FROM up;

    WITH del AS (
        DELETE FROM gold.hourly_sensor_pm25 t
        WHERE NOT EXISTS (SELECT 1 FROM _g_hourly g
                          WHERE g.sensor_id = t.sensor_id AND g.hour_start_utc = t.hour_start_utc)
        RETURNING 1
    )
    SELECT count(*) INTO v_hd FROM del;

    WITH up AS (
        INSERT INTO gold.daily_location_summary AS t
               (location_id, local_date, location_name, timezone, latitude, longitude, avg_pm25,
                avg_pm25_excl_flatline, min_hourly_avg, max_hourly_avg, max_reading,
                n_sensors_reporting, n_sensors_at_location, n_readings, n_location_hours,
                expected_seconds, covered_seconds, coverage_pct, is_partial_day, meets_completeness,
                n_flatline_readings, n_high_value_readings, gold_run_id)
        SELECT g.location_id, g.local_date, g.location_name, g.timezone, g.latitude, g.longitude, g.avg_pm25,
               g.avg_pm25_excl_flatline, g.min_hourly_avg, g.max_hourly_avg, g.max_reading,
               g.n_sensors_reporting, g.n_sensors_at_location, g.n_readings, g.n_location_hours,
               g.expected_seconds, g.covered_seconds, g.coverage_pct, g.is_partial_day, g.meets_completeness,
               g.n_flatline_readings, g.n_high_value_readings, p_run_id
        FROM _g_daily g
        ON CONFLICT (location_id, local_date) DO UPDATE SET
               location_name = EXCLUDED.location_name, timezone = EXCLUDED.timezone,
               latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude,
               avg_pm25 = EXCLUDED.avg_pm25, avg_pm25_excl_flatline = EXCLUDED.avg_pm25_excl_flatline,
               min_hourly_avg = EXCLUDED.min_hourly_avg, max_hourly_avg = EXCLUDED.max_hourly_avg,
               max_reading = EXCLUDED.max_reading, n_sensors_reporting = EXCLUDED.n_sensors_reporting,
               n_sensors_at_location = EXCLUDED.n_sensors_at_location, n_readings = EXCLUDED.n_readings,
               n_location_hours = EXCLUDED.n_location_hours, expected_seconds = EXCLUDED.expected_seconds,
               covered_seconds = EXCLUDED.covered_seconds, coverage_pct = EXCLUDED.coverage_pct,
               is_partial_day = EXCLUDED.is_partial_day, meets_completeness = EXCLUDED.meets_completeness,
               n_flatline_readings = EXCLUDED.n_flatline_readings,
               n_high_value_readings = EXCLUDED.n_high_value_readings,
               gold_run_id = EXCLUDED.gold_run_id, built_at = now()
        WHERE (t.location_name, t.timezone, t.latitude, t.longitude, t.avg_pm25, t.avg_pm25_excl_flatline,
               t.min_hourly_avg, t.max_hourly_avg, t.max_reading, t.n_sensors_reporting,
               t.n_sensors_at_location, t.n_readings, t.n_location_hours, t.expected_seconds,
               t.covered_seconds, t.coverage_pct, t.is_partial_day, t.meets_completeness,
               t.n_flatline_readings, t.n_high_value_readings)
              IS DISTINCT FROM
              (EXCLUDED.location_name, EXCLUDED.timezone, EXCLUDED.latitude, EXCLUDED.longitude,
               EXCLUDED.avg_pm25, EXCLUDED.avg_pm25_excl_flatline, EXCLUDED.min_hourly_avg,
               EXCLUDED.max_hourly_avg, EXCLUDED.max_reading, EXCLUDED.n_sensors_reporting,
               EXCLUDED.n_sensors_at_location, EXCLUDED.n_readings, EXCLUDED.n_location_hours,
               EXCLUDED.expected_seconds, EXCLUDED.covered_seconds, EXCLUDED.coverage_pct,
               EXCLUDED.is_partial_day, EXCLUDED.meets_completeness, EXCLUDED.n_flatline_readings,
               EXCLUDED.n_high_value_readings)
        RETURNING (xmax = 0) AS ins
    )
    SELECT count(*) FILTER (WHERE ins), count(*) FILTER (WHERE NOT ins) INTO v_di, v_du FROM up;

    WITH del AS (
        DELETE FROM gold.daily_location_summary t
        WHERE NOT EXISTS (SELECT 1 FROM _g_daily g
                          WHERE g.location_id = t.location_id AND g.local_date = t.local_date)
        RETURNING 1
    )
    SELECT count(*) INTO v_dd FROM del;

    WITH up AS (
        INSERT INTO gold.sensor_health AS t
               (sensor_id, local_date, location_id, timezone, expected_interval_seconds, expected_seconds,
                expected_readings, received_readings, covered_seconds, coverage_pct, longest_gap_minutes,
                n_flatline_readings, n_high_value_readings, n_long_gap_readings, flatline_pct,
                first_reading_utc, last_reading_utc, is_partial_day, health_status, gold_run_id)
        SELECT g.sensor_id, g.local_date, g.location_id, g.timezone, g.expected_interval_seconds,
               g.expected_seconds, g.expected_readings, g.received_readings, g.covered_seconds,
               g.coverage_pct, g.longest_gap_minutes, g.n_flatline_readings, g.n_high_value_readings,
               g.n_long_gap_readings, g.flatline_pct, g.first_reading_utc, g.last_reading_utc,
               g.is_partial_day, g.health_status, p_run_id
        FROM _g_health g
        ON CONFLICT (sensor_id, local_date) DO UPDATE SET
               location_id = EXCLUDED.location_id, timezone = EXCLUDED.timezone,
               expected_interval_seconds = EXCLUDED.expected_interval_seconds,
               expected_seconds = EXCLUDED.expected_seconds, expected_readings = EXCLUDED.expected_readings,
               received_readings = EXCLUDED.received_readings, covered_seconds = EXCLUDED.covered_seconds,
               coverage_pct = EXCLUDED.coverage_pct, longest_gap_minutes = EXCLUDED.longest_gap_minutes,
               n_flatline_readings = EXCLUDED.n_flatline_readings,
               n_high_value_readings = EXCLUDED.n_high_value_readings,
               n_long_gap_readings = EXCLUDED.n_long_gap_readings, flatline_pct = EXCLUDED.flatline_pct,
               first_reading_utc = EXCLUDED.first_reading_utc, last_reading_utc = EXCLUDED.last_reading_utc,
               is_partial_day = EXCLUDED.is_partial_day, health_status = EXCLUDED.health_status,
               gold_run_id = EXCLUDED.gold_run_id, built_at = now()
        WHERE (t.location_id, t.timezone, t.expected_interval_seconds, t.expected_seconds,
               t.expected_readings, t.received_readings, t.covered_seconds, t.coverage_pct,
               t.longest_gap_minutes, t.n_flatline_readings, t.n_high_value_readings,
               t.n_long_gap_readings, t.flatline_pct, t.first_reading_utc, t.last_reading_utc,
               t.is_partial_day, t.health_status)
              IS DISTINCT FROM
              (EXCLUDED.location_id, EXCLUDED.timezone, EXCLUDED.expected_interval_seconds,
               EXCLUDED.expected_seconds, EXCLUDED.expected_readings, EXCLUDED.received_readings,
               EXCLUDED.covered_seconds, EXCLUDED.coverage_pct, EXCLUDED.longest_gap_minutes,
               EXCLUDED.n_flatline_readings, EXCLUDED.n_high_value_readings, EXCLUDED.n_long_gap_readings,
               EXCLUDED.flatline_pct, EXCLUDED.first_reading_utc, EXCLUDED.last_reading_utc,
               EXCLUDED.is_partial_day, EXCLUDED.health_status)
        RETURNING (xmax = 0) AS ins
    )
    SELECT count(*) FILTER (WHERE ins), count(*) FILTER (WHERE NOT ins) INTO v_ei, v_eu FROM up;

    WITH del AS (
        DELETE FROM gold.sensor_health t
        WHERE NOT EXISTS (SELECT 1 FROM _g_health g
                          WHERE g.sensor_id = t.sensor_id AND g.local_date = t.local_date)
        RETURNING 1
    )
    SELECT count(*) INTO v_ed FROM del;

    ------------------------------------------------------------------ 5. reconciliation guards
    -- Any failure raises, rolling back the WHOLE run (one transaction).
    SELECT COALESCE(SUM(n_readings),0), COALESCE(SUM(n_flatline_readings),0), COALESCE(SUM(n_high_value_readings),0),
           count(*)
      INTO v_h_n, v_h_flat, v_h_high, v_h_total
      FROM gold.hourly_sensor_pm25;
    SELECT COALESCE(SUM(n_readings),0), COALESCE(SUM(n_flatline_readings),0), COALESCE(SUM(n_high_value_readings),0),
           count(*)
      INTO v_d_n, v_d_flat, v_d_high, v_d_total
      FROM gold.daily_location_summary;
    SELECT COALESCE(SUM(received_readings),0), count(*) INTO v_e_n, v_e_total FROM gold.sensor_health;

    IF v_h_n <> v_silver THEN
        RAISE EXCEPTION 'G1 failed: sum(hourly.n_readings)=% but silver readings=%', v_h_n, v_silver;
    END IF;
    IF v_h_flat <> v_s_flat OR v_h_high <> v_s_high THEN
        RAISE EXCEPTION 'G2 failed: hourly flatline/high=%/% but silver=%/%', v_h_flat, v_h_high, v_s_flat, v_s_high;
    END IF;
    IF v_d_n <> v_h_n OR v_d_flat <> v_h_flat OR v_d_high <> v_h_high THEN
        RAISE EXCEPTION 'G3 failed: daily n/flat/high=%/%/% but hourly=%/%/%',
                        v_d_n, v_d_flat, v_d_high, v_h_n, v_h_flat, v_h_high;
    END IF;
    IF v_e_n <> v_silver THEN
        RAISE EXCEPTION 'G4 failed: sum(health.received_readings)=% but silver readings=%', v_e_n, v_silver;
    END IF;

    SELECT (SELECT count(*) FROM gold.hourly_sensor_pm25
             WHERE n_readings < 1 OR coverage_pct NOT BETWEEN 0 AND 100 OR covered_seconds NOT BETWEEN 0 AND 3600)
         + (SELECT count(*) FROM gold.daily_location_summary
             WHERE n_location_hours < 1 OR coverage_pct NOT BETWEEN 0 AND 100)
         + (SELECT count(*) FROM gold.sensor_health WHERE coverage_pct NOT BETWEEN 0 AND 100)
      INTO v_bad_rows;
    IF v_bad_rows > 0 THEN
        RAISE EXCEPTION 'G5 failed: % Gold rows violate coverage/readings bounds', v_bad_rows;
    END IF;

    -- G6 (extra): the health table must contain EXACTLY the sensor x day rows that were computed,
    -- and every sensor must appear, so silent sensors cannot vanish.
    SELECT count(*) INTO v_e_expected FROM _g_health;
    SELECT count(*) INTO v_e_missing FROM silver.sensors s
     WHERE v_h0 IS NOT NULL AND NOT EXISTS (SELECT 1 FROM gold.sensor_health h WHERE h.sensor_id = s.sensor_id);
    IF v_e_total <> v_e_expected OR v_e_missing > 0 THEN
        RAISE EXCEPTION 'G6 failed: health rows=% expected=% sensors_without_rows=%',
                        v_e_total, v_e_expected, v_e_missing;
    END IF;

    RETURN QUERY SELECT v_hi, v_hu, v_hd, v_di, v_du, v_dd, v_ei, v_eu, v_ed,
                        v_h_total, v_d_total, v_e_total, v_silver, v_h0, v_h1;
END
$$;
