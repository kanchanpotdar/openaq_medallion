-- 08 - Flatline (stuck sensor) analysis
-- Demonstrates: gaps-and-islands with LAG() and a running SUM() to number consecutive runs.
-- A FLATLINE reading belongs to a run of >= 8 identical readings spanning >= 4 hours. Each run is rebuilt
-- here from the flagged rows: a new run starts when the value changes or the gap exceeds twice the interval.

WITH f AS (
    SELECT sensor_id, pm25_ugm3, period_from_utc, period_to_utc,
           CASE WHEN lag(pm25_ugm3) OVER w = pm25_ugm3
                 AND period_from_utc - lag(period_to_utc) OVER w <= 2 * make_interval(secs => duration_seconds)
                THEN 0 ELSE 1 END AS new_run
    FROM silver.measurements
    WHERE is_flatline
    WINDOW w AS (PARTITION BY sensor_id ORDER BY period_from_utc)
), r AS (
    SELECT *, sum(new_run) OVER (PARTITION BY sensor_id ORDER BY period_from_utc) AS run_no
    FROM f
)
SELECT sensor_id, pm25_ugm3 AS stuck_value, count(*) AS readings,
       min(period_from_utc) AS run_start, max(period_to_utc) AS run_end,
       round(EXTRACT(EPOCH FROM max(period_to_utc) - min(period_from_utc)) / 3600, 1) AS hours
FROM r
GROUP BY sensor_id, run_no, pm25_ugm3
ORDER BY readings DESC
LIMIT 15;
