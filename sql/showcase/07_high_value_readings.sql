-- 07 - High-value readings in context
-- Demonstrates: an ordered-set aggregate (percentile_cont) in a CTE, joins, and AT TIME ZONE.
-- HIGH_VALUE readings (> 500 ug/m3) are kept and flagged, never dropped. Comparing each one with its own
-- sensor's median separates a plausible pollution episode from a sensor that always reads high.

WITH med AS (
    SELECT sensor_id, percentile_cont(0.5) WITHIN GROUP (ORDER BY pm25_ugm3) AS median
    FROM silver.measurements
    GROUP BY sensor_id
)
SELECT m.sensor_id, s.provider_name,
       m.period_from_utc AT TIME ZONE s.timezone                  AS local_time,
       m.pm25_ugm3,
       round(med.median::numeric, 1)                              AS sensor_median,
       round((m.pm25_ugm3 / NULLIF(med.median, 0))::numeric, 1)   AS times_median,
       m.is_flatline
FROM silver.measurements m
JOIN silver.sensors s USING (sensor_id)
JOIN med USING (sensor_id)
WHERE m.is_high_value
ORDER BY m.pm25_ugm3 DESC
LIMIT 20;
