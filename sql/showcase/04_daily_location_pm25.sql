-- 04 - Daily location PM2.5 trend
-- Demonstrates: a named WINDOW, LAG(), a rolling average with a ROWS frame, and RANK() per partition.
-- Only complete, full local days are used (meets_completeness AND NOT is_partial_day), so incomplete days
-- cannot distort the trend. "Previous day" means the previous AVAILABLE day for that location.

WITH d AS (
    SELECT location_id, location_name, local_date, avg_pm25, n_sensors_reporting
    FROM gold.daily_location_summary
    WHERE meets_completeness AND NOT is_partial_day
)
SELECT location_id, location_name, local_date, avg_pm25,
       round(avg_pm25 - lag(avg_pm25) OVER w, 2)                              AS change_vs_previous_available_day,
       round(avg(avg_pm25) OVER (w ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS rolling_3_available_day_avg,
       rank() OVER (PARTITION BY local_date ORDER BY avg_pm25 DESC)           AS pollution_rank_that_day
FROM d
WINDOW w AS (PARTITION BY location_id ORDER BY local_date)
ORDER BY local_date DESC, pollution_rank_that_day
LIMIT 20;
