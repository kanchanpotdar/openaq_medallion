-- 05 - Incomplete sensors and days, and why there is no data
-- Demonstrates: filtering on derived health fields, joining Gold to a Silver dimension, and a view that
-- explains NO_DATA from the audit trail.

-- (a) The worst full days for sensors that did report: low coverage and long gaps.
SELECT h.local_date, h.sensor_id, s.provider_name, h.health_status, h.coverage_pct,
       h.received_readings, h.expected_readings, h.longest_gap_minutes
FROM gold.sensor_health h
JOIN silver.sensors s USING (sensor_id)
WHERE h.health_status IN ('PARTIAL', 'LOW_COVERAGE') AND NOT h.is_partial_day
ORDER BY h.coverage_pct, h.longest_gap_minutes DESC
LIMIT 25;

-- (b) Every NO_DATA sensor-day with the most specific reason the audit trail supports
--     (fetch failed vs source returned nothing vs skipped vs provider stopped ...).
SELECT no_data_reason, count(*) AS sensor_days, count(DISTINCT sensor_id) AS sensors
FROM audit.v_no_data_reason
GROUP BY no_data_reason
ORDER BY sensor_days DESC;
