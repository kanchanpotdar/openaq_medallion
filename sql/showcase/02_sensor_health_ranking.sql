-- 02 - Sensor health ranking
-- Demonstrates: aggregate FILTER, HAVING, and RANK() over an aggregate (a window over grouped rows).
-- Ranks sensors that reported at all by their average daily coverage. Coverage is time-based, so 5-,
-- 15- and 60-minute sensors are directly comparable; stuck_days exposes sensors that look healthy
-- (coverage ~100%) while reporting a frozen value.

SELECT sensor_id,
       count(*) FILTER (WHERE received_readings > 0)                          AS days_reporting,
       round(avg(coverage_pct) FILTER (WHERE received_readings > 0), 1)       AS avg_coverage_pct,
       max(longest_gap_minutes)                                               AS worst_gap_minutes,
       count(*) FILTER (WHERE health_status = 'STUCK')                        AS stuck_days,
       rank() OVER (ORDER BY avg(coverage_pct) FILTER (WHERE received_readings > 0) DESC NULLS LAST)
                                                                              AS coverage_rank
FROM gold.sensor_health
GROUP BY sensor_id
HAVING count(*) FILTER (WHERE received_readings > 0) > 0
ORDER BY coverage_rank, sensor_id
LIMIT 20;
