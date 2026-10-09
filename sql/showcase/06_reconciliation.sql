-- 06 - Reconciliation across layers
-- Demonstrates: scalar subqueries, FILTER aggregates, and reading the durable audit checks.
-- Identity: bronze = valid + rejected + duplicate = classified, and every Gold layer sums back to Silver.

-- (a) One row that should show matching numbers all the way down.
SELECT (SELECT count(*) FROM bronze.measurements)                        AS bronze,
       count(*) FILTER (WHERE classification = 'VALID')                  AS valid,
       count(*) FILTER (WHERE classification = 'REJECTED')               AS rejected,
       count(*) FILTER (WHERE classification = 'DUPLICATE')              AS duplicate,
       (SELECT count(*) FROM silver.measurements)                        AS silver_rows,
       (SELECT sum(n_readings) FROM gold.hourly_sensor_pm25)             AS gold_hourly_readings,
       (SELECT sum(n_readings) FROM gold.daily_location_summary)         AS gold_daily_readings,
       (SELECT sum(received_readings) FROM gold.sensor_health)           AS gold_health_readings
FROM silver.bronze_classification;

-- (b) The latest successful run's checks that are not a clean PASS (empty = everything reconciled).
SELECT check_name, severity, status, expected_value, actual_value, details
FROM audit.reconciliation_checks
WHERE run_id = (SELECT run_id FROM audit.pipeline_runs
                WHERE status = 'SUCCESS' ORDER BY finished_at DESC LIMIT 1)
  AND status <> 'PASS'
ORDER BY check_id;
