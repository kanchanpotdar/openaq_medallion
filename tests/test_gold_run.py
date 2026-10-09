"""Run-level Gold properties: reconciliation, idempotency, stale deletion, guards, atomicity, locks."""
from datetime import datetime, timezone

import psycopg
import pytest

from pipeline import config, run_gold, run_silver
from tests import gold_helpers as g
from tests import helpers

pytestmark = pytest.mark.usefixtures("gold_sensors")
ist = g.ist
UTC = timezone.utc
ZERO_WRITES = dict(out_hourly_inserted=0, out_hourly_updated=0, out_hourly_deleted=0,
                   out_daily_inserted=0, out_daily_updated=0, out_daily_deleted=0,
                   out_health_inserted=0, out_health_updated=0, out_health_deleted=0)


@pytest.fixture
def scenario(db):
    """Dirty fixture (rejects + duplicates + valid) plus clean series, with a defined horizon."""
    helpers.load_cases(db)
    g.default_horizon(db)
    helpers.series(db, 1003, ist(2026, 10, 4, 6), 15, g.varied(4))
    helpers.series(db, 1005, ist(2026, 10, 4, 0), 15, [600.0] * 16)       # flatline + high value
    db.commit()
    return db


def _sum(db, sql):
    return db.execute(sql).fetchone()[0]


# ---------------------------------------------------------------- reconciliation

def test_gold_reconciles_to_silver_at_every_layer(scenario):
    db = scenario
    g.build(db)
    silver = _sum(db, "SELECT count(*) FROM silver.measurements")
    assert _sum(db, "SELECT sum(n_readings) FROM gold.hourly_sensor_pm25") == silver             # G1
    assert _sum(db, "SELECT sum(n_flatline_readings) FROM gold.hourly_sensor_pm25") == \
        _sum(db, "SELECT count(*) FROM silver.measurements WHERE is_flatline")                  # G2
    assert _sum(db, "SELECT sum(n_high_value_readings) FROM gold.hourly_sensor_pm25") == \
        _sum(db, "SELECT count(*) FROM silver.measurements WHERE is_high_value")
    assert _sum(db, "SELECT sum(n_readings) FROM gold.daily_location_summary") == silver        # G3
    assert _sum(db, "SELECT sum(received_readings) FROM gold.sensor_health") == silver          # G4
    assert silver > 0 and _sum(db, "SELECT sum(n_flatline_readings) FROM gold.hourly_sensor_pm25") > 0


# ---------------------------------------------------------------- idempotency

def test_second_run_writes_zero_rows_and_keeps_the_original_run_id(scenario):
    db = scenario
    first = g.build(db)
    before = g.snapshot(db)
    assert first["out_hourly_inserted"] > 0 and first["out_health_inserted"] > 0

    second = g.build(db)

    assert {k: second[k] for k in ZERO_WRITES} == ZERO_WRITES
    assert g.snapshot(db) == before
    # untouched rows keep the run id that last CHANGED them: lineage is not rewritten by no-op runs
    runs = _sum(db, "SELECT count(DISTINCT gold_run_id) FROM gold.hourly_sensor_pm25")
    assert runs == 1


def test_a_late_reading_changes_only_the_affected_rows(scenario):
    db = scenario
    g.build(db)
    before = g.snapshot(db)
    helpers.reading(db, 1003, ist(2026, 10, 4, 9), 15, 7)         # a new hour for a sensor that already has data that day
    stats = g.build(db)
    assert (stats["out_hourly_inserted"], stats["out_hourly_updated"], stats["out_hourly_deleted"]) == (1, 0, 0)
    assert (stats["out_daily_inserted"], stats["out_daily_updated"]) == (0, 1)       # same location-day, new mean
    assert (stats["out_health_inserted"], stats["out_health_updated"]) == (0, 1)     # same sensor-day, one more reading
    changed = [i for i, (a, b) in enumerate(zip(before, g.snapshot(db))) if a != b]
    assert changed == [0, 1, 2]                                   # all three tables moved, by one row each


def test_rows_that_silver_no_longer_supports_are_deleted_from_gold(scenario):
    db = scenario
    g.build(db)
    f01 = db.execute("SELECT bronze_id FROM bronze.measurements WHERE source_url = 'fixture://F01_same_time_sensor_1001'").fetchone()[0]
    db.execute("DELETE FROM silver.measurements WHERE bronze_id = %s", (f01,))       # the only reading in its hour and day
    db.commit()
    stats = run_gold.run(db)
    db.commit()
    assert (stats["out_hourly_deleted"], stats["out_daily_deleted"], stats["out_health_deleted"]) == (1, 1, 0)
    assert stats["out_health_updated"] >= 1                       # its health row is kept, now with 0 received
    assert _sum(db, "SELECT sum(n_readings) FROM gold.hourly_sensor_pm25") == \
        _sum(db, "SELECT count(*) FROM silver.measurements")


def test_silver_full_refresh_still_works_after_gold_exists(scenario):
    """Gold must not hold foreign keys into Silver: Silver's TRUNCATE-based full refresh would break."""
    db = scenario
    g.build(db)
    before = g.snapshot(db)
    run_silver.run(db, full_refresh=True)
    stats = run_gold.run(db)
    db.commit()
    assert {k: stats[k] for k in ZERO_WRITES} == ZERO_WRITES
    assert g.snapshot(db) == before


def test_empty_silver_builds_empty_gold_without_error(db):
    stats = g.build(db)
    assert (stats["out_hourly_total"], stats["out_daily_total"], stats["out_health_total"]) == (0, 0, 0)
    assert stats["out_horizon_from"] is None


# ---------------------------------------------------------------- validation

@pytest.mark.parametrize("tz, shown", [("Mars/Phobos", "Mars/Phobos"), (None, "<NULL>")])
def test_invalid_or_missing_timezone_aborts_and_names_the_sensor(db, tz, shown):
    helpers.insert_sensor(db, 1090, timezone=tz)
    helpers.load_cases(db)
    run_silver.run(db)
    db.commit()
    with pytest.raises(psycopg.errors.RaiseException, match="invalid or NULL timezone") as e:
        run_gold.run(db)
    assert shown in str(e.value) and "1090" in str(e.value)
    db.rollback()
    assert g.gold_counts(db) == dict(hourly=0, daily=0, health=0)


def test_a_horizon_that_does_not_cover_the_data_is_rejected(scenario):
    db = scenario
    run_silver.run(db)
    db.commit()
    with pytest.raises(psycopg.errors.RaiseException, match="does not cover"):
        run_gold.run(db, p_horizon_from=datetime(2030, 1, 1, tzinfo=UTC))
    db.rollback()


def test_an_explicit_wider_horizon_is_honoured(scenario):
    db = scenario
    run_silver.run(db)
    wide_from, wide_to = datetime(2026, 9, 30, tzinfo=UTC), datetime(2026, 10, 10, tzinfo=UTC)
    stats = run_gold.run(db, p_horizon_from=wide_from, p_horizon_to=wide_to)
    db.commit()
    assert stats["out_horizon_from"].startswith("2026-09-30") and stats["out_horizon_to"].startswith("2026-10-10")


# ---------------------------------------------------------------- guards + atomicity

def _install_trigger(db, table, body):
    short = table.split(".")[1]
    db.execute(f"CREATE OR REPLACE FUNCTION gold._sabotage() RETURNS trigger LANGUAGE plpgsql AS "
               f"$$ BEGIN {body} RETURN NEW; END $$")
    db.execute(f"CREATE TRIGGER trg_sabotage BEFORE INSERT ON {table} "
               f"FOR EACH ROW EXECUTE FUNCTION gold._sabotage()")
    db.commit()
    return short


def _remove_trigger(db, table):
    db.rollback()
    db.execute(f"DROP TRIGGER IF EXISTS trg_sabotage ON {table}")
    db.execute("DROP FUNCTION IF EXISTS gold._sabotage()")
    db.commit()


GUARDS = [
    ("G1", "gold.hourly_sensor_pm25", "NEW.n_readings := NEW.n_readings + 1;"),
    ("G2", "gold.hourly_sensor_pm25", "NEW.n_flatline_readings := NEW.n_flatline_readings + 1;"),
    ("G3", "gold.daily_location_summary", "NEW.n_readings := NEW.n_readings + 1;"),
    ("G4", "gold.sensor_health", "NEW.received_readings := NEW.received_readings + 1;"),
    ("G6", "gold.sensor_health", "IF NEW.received_readings = 0 THEN RETURN NULL; END IF;"),
]


@pytest.mark.parametrize("guard, table, body", GUARDS, ids=[x[0] for x in GUARDS])
def test_each_reconciliation_guard_fails_the_run_and_rolls_it_back(scenario, guard, table, body):
    db = scenario
    run_silver.run(db)
    db.commit()
    _install_trigger(db, table, body)
    try:
        with pytest.raises(psycopg.errors.RaiseException, match=f"{guard} failed"):
            run_gold.run(db)
        db.rollback()
        assert g.gold_counts(db) == dict(hourly=0, daily=0, health=0)       # nothing from the failed run survives
    finally:
        _remove_trigger(db, table)
    g.build(db)                                                             # and a clean retry succeeds
    assert g.gold_counts(db)["hourly"] > 0


def test_guard_g5_catches_out_of_range_coverage_even_without_the_check_constraint(scenario):
    db = scenario
    run_silver.run(db)
    db.commit()
    db.execute("ALTER TABLE gold.hourly_sensor_pm25 DROP CONSTRAINT ck_hourly_coverage")
    db.commit()
    _install_trigger(db, "gold.hourly_sensor_pm25", "NEW.coverage_pct := 150;")
    try:
        with pytest.raises(psycopg.errors.RaiseException, match="G5 failed"):
            run_gold.run(db)
        db.rollback()
    finally:
        _remove_trigger(db, "gold.hourly_sensor_pm25")
        db.execute("ALTER TABLE gold.hourly_sensor_pm25 "
                   "ADD CONSTRAINT ck_hourly_coverage CHECK (coverage_pct BETWEEN 0 AND 100)")
        db.commit()


def test_failure_midway_leaves_no_partial_gold(scenario):
    """Sabotage the SECOND table written (daily): the hourly rows already written must roll back too."""
    db = scenario
    run_silver.run(db)
    db.commit()
    _install_trigger(db, "gold.daily_location_summary", "RAISE EXCEPTION 'boom';")
    try:
        with pytest.raises(psycopg.errors.RaiseException, match="boom"):
            run_gold.run(db)
        db.rollback()
        assert g.gold_counts(db) == dict(hourly=0, daily=0, health=0)
    finally:
        _remove_trigger(db, "gold.daily_location_summary")


# ---------------------------------------------------------------- concurrency

def test_gold_waits_for_a_running_silver_build(scenario):
    """Gold takes Silver's advisory lock, so it can never read a half-finished Silver run."""
    db = scenario
    run_silver.run(db)
    db.commit()
    other = psycopg.connect(**config.db_kwargs(helpers.TEST_DB), autocommit=True)
    try:
        other.execute("SELECT pg_advisory_lock(hashtext('silver.run_silver'))")
        db.execute("SET lock_timeout = '300ms'")
        with pytest.raises(psycopg.errors.LockNotAvailable):
            run_gold.run(db)
        db.rollback()
    finally:
        other.execute("SELECT pg_advisory_unlock_all()")
        other.close()
    db.execute("SET lock_timeout = 0")
    g.build(db)                                                              # proceeds once the lock is free
    assert g.gold_counts(db)["hourly"] > 0
