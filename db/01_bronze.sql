-- 01_bronze.sql : immutable raw storage. Idempotent.
-- Rule: everything the source sent is kept as TEXT / JSONB. No cleaning here.

CREATE TABLE IF NOT EXISTS bronze.sensors (
    bronze_id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    -- raw source fields (TEXT: we do not trust types yet)
    sensor_id          TEXT,
    sensor_name        TEXT,
    parameter_id       TEXT,
    parameter_name     TEXT,
    units              TEXT,
    location_id        TEXT,
    location_name      TEXT,
    locality           TEXT,
    provider_name      TEXT,
    country_code       TEXT,
    timezone           TEXT,
    latitude           TEXT,
    longitude          TEXT,
    is_monitor         TEXT,
    is_mobile          TEXT,
    datetime_first_utc TEXT,
    datetime_last_utc  TEXT,
    raw_json           JSONB       NOT NULL,
    -- lineage
    source_url         TEXT        NOT NULL,
    run_id             UUID        NOT NULL,
    ingested_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    row_hash           TEXT        NOT NULL,
    CONSTRAINT uq_bronze_sensors_row_hash UNIQUE (row_hash)
);

CREATE TABLE IF NOT EXISTS bronze.measurements (
    bronze_id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sensor_id           TEXT,       -- from the request path (payload does not carry it)
    parameter_id        TEXT,
    value_raw           TEXT,
    unit_raw            TEXT,
    period_label        TEXT,
    period_interval_raw TEXT,
    period_from_raw     TEXT,       -- datetimeFrom.utc as sent
    period_to_raw       TEXT,       -- datetimeTo.utc as sent
    has_flags_raw       TEXT,
    raw_json            JSONB       NOT NULL,
    source_url          TEXT        NOT NULL,
    run_id              UUID        NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    row_hash            TEXT        NOT NULL,
    CONSTRAINT uq_bronze_measurements_row_hash UNIQUE (row_hash)
);

-- Lineage lookups: "which rows did run X load?"
CREATE INDEX IF NOT EXISTS ix_bronze_sensors_run      ON bronze.sensors      (run_id);
CREATE INDEX IF NOT EXISTS ix_bronze_measurements_run ON bronze.measurements (run_id);

-- Enforce immutability: Bronze rows can be inserted, never changed or removed.
CREATE OR REPLACE FUNCTION bronze.forbid_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'bronze.% is immutable: % is not allowed', TG_TABLE_NAME, TG_OP;
END
$$;

DROP TRIGGER IF EXISTS trg_sensors_immutable ON bronze.sensors;
CREATE TRIGGER trg_sensors_immutable BEFORE UPDATE OR DELETE ON bronze.sensors
    FOR EACH ROW EXECUTE FUNCTION bronze.forbid_change();

DROP TRIGGER IF EXISTS trg_measurements_immutable ON bronze.measurements;
CREATE TRIGGER trg_measurements_immutable BEFORE UPDATE OR DELETE ON bronze.measurements
    FOR EACH ROW EXECUTE FUNCTION bronze.forbid_change();
