-- 03 - Provider freshness
-- Demonstrates: a view over the audit trail, a scalar subquery to pick the latest successful run.
-- How far behind the requested window end is each provider's newest data? A whole provider that stops
-- early is source evidence, which is different from individual sensors failing to fetch.

SELECT provider_name, sensors, fetched_with_data, zero_rows, failed, skipped_inactive,
       last_data_utc, hours_behind_window_end
FROM audit.v_provider_freshness
WHERE run_id = (SELECT run_id FROM audit.pipeline_runs
                WHERE status = 'SUCCESS' ORDER BY finished_at DESC LIMIT 1)
  AND (fetched_with_data > 0 OR failed > 0)
ORDER BY hours_behind_window_end DESC NULLS LAST;
