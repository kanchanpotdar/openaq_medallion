-- 01 - Data quality overview
-- Demonstrates: GROUP BY, a window function used for percentages (sum(count(*)) OVER ()), FILTER aggregates.
-- Every Bronze row has exactly one verdict; soft flags annotate VALID rows instead of removing them.

-- (a) Verdicts for every Bronze row, with the reason for rejections.
SELECT classification,
       COALESCE(reason_code, '-')                                   AS reason,
       count(*)                                                     AS rows,
       round(100.0 * count(*) / sum(count(*)) OVER (), 3)           AS pct
FROM silver.bronze_classification
GROUP BY classification, reason_code
ORDER BY rows DESC;

-- (b) Soft quality flags on the VALID rows.
SELECT count(*)                                                     AS valid_rows,
       count(*) FILTER (WHERE is_high_value)                        AS high_value,
       count(*) FILTER (WHERE is_long_gap)                          AS long_gap,
       count(*) FILTER (WHERE is_flatline)                          AS flatline,
       round(100.0 * count(*) FILTER (WHERE is_high_value OR is_long_gap OR is_flatline)
             / count(*), 2)                                         AS pct_with_any_flag
FROM silver.measurements;
