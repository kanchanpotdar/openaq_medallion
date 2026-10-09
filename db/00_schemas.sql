-- 00_schemas.sql : one schema per medallion layer + audit.
-- Idempotent: safe to run more than once.

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS audit;

COMMENT ON SCHEMA bronze IS 'Immutable raw API data + lineage. Never transformed.';
COMMENT ON SCHEMA silver IS 'Typed, validated, deduplicated data; rejected rows and per-row classification.';
COMMENT ON SCHEMA gold   IS 'Analytics-ready aggregates, rebuilt idempotently from Silver.';
COMMENT ON SCHEMA audit  IS 'Pipeline run history and reconciliation results.';

-- Store and compare all timestamps in UTC so session settings never change results.
DO $$
BEGIN
    EXECUTE format('ALTER DATABASE %I SET timezone TO %L', current_database(), 'UTC');
END
$$;
