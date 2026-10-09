"""Run-level properties: reconciliation, idempotency, incremental processing, full refresh,
atomicity, the reconciliation guard, sensor dimension, and Bronze immutability by Silver."""
from datetime import datetime, timezone

import psycopg
import pytest

from pipeline import run_silver
from tests import helpers

T0 = datetime(2026, 10, 4, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def loaded(db):
    ids = helpers.load_cases(db)
    return db, ids


def _expected_counts():
    cases = helpers.read_cases()
    return {k: sum(c["expected_class"] == k.upper() for c in cases)
            for k in ("valid", "rejected", "duplicate")} | {"bronze": len(cases)}


# ---------------------------------------------------------------- reconciliation

def test_bronze_equals_valid_plus_rejected_plus_duplicate(loaded):
    db, _ = loaded
    stats = run_silver.run(db)
    db.commit()
    c = helpers.counts(db)
    exp = _expected_counts()
    assert c["bronze"] == exp["bronze"]
    assert c["bronze"] == c["valid"] + c["rejected"] + c["duplicate"]
    assert (c["valid"], c["rejected"], c["duplicate"]) == (exp["valid"], exp["rejected"], exp["duplicate"])
    # the function reports the same totals it enforces
    assert stats["out_bronze_total"] == c["bronze"]
    assert stats["out_classified"] == c["bronze"]


def test_every_bronze_row_has_exactly_one_verdict_and_one_destination(loaded):
    db, _ = loaded
    run_silver.run(db)
    db.commit()
    c = helpers.counts(db)
    assert c["classified"] == c["bronze"]
    unclassified = db.execute(
        "SELECT count(*) FROM bronze.measurements m WHERE NOT EXISTS "
        "(SELECT 1 FROM silver.bronze_classification c WHERE c.bronze_id = m.bronze_id)").fetchone()[0]
    assert unclassified == 0
    # destination tables line up with the verdicts
    assert c["silver_rows"] == c["valid"]
    assert c["rejected_rows"] == c["rejected"]
    mismatched = db.execute(
        "SELECT count(*) FROM silver.bronze_classification c "
        "LEFT JOIN silver.measurements m USING (bronze_id) "
        "LEFT JOIN silver.rejected_records r USING (bronze_id) "
        "WHERE (c.classification='VALID')    <> (m.bronze_id IS NOT NULL) "
        "   OR (c.classification='REJECTED') <> (r.bronze_id IS NOT NULL) "
        "   OR (c.classification='REJECTED' AND r.reason_code IS DISTINCT FROM c.reason_code)").fetchone()[0]
    assert mismatched == 0


def test_every_duplicate_points_at_a_valid_row_with_the_same_key(loaded):
    db, _ = loaded
    run_silver.run(db)
    db.commit()
    bad = db.execute(
        "SELECT count(*) FROM silver.bronze_classification c "
        "JOIN bronze.measurements d ON d.bronze_id = c.bronze_id "
        "JOIN silver.measurements w ON w.bronze_id = c.duplicate_of_bronze_id "
        "WHERE c.classification='DUPLICATE' "
        "  AND (w.sensor_id::text <> d.sensor_id OR w.period_from_utc <> d.period_from_raw::timestamptz "
        "       OR w.bronze_id >= d.bronze_id)").fetchone()[0]
    assert bad == 0   # same key, and the winner is the EARLIER bronze_id


def test_silver_never_modifies_bronze(loaded):
    db, _ = loaded
    checksum = ("SELECT md5(string_agg(bronze_id::text||row_hash||coalesce(value_raw,'~')||"
                "coalesce(period_from_raw,'~'), ',' ORDER BY bronze_id)) FROM bronze.measurements")
    before = db.execute(checksum).fetchone()[0]
    run_silver.run(db)
    run_silver.run(db, full_refresh=True)
    db.commit()
    assert db.execute(checksum).fetchone()[0] == before


# ---------------------------------------------------------------- idempotency

def test_second_run_changes_nothing(loaded):
    db, _ = loaded
    run_silver.run(db)
    db.commit()
    before = helpers.snapshot(db)

    stats = run_silver.run(db)
    db.commit()

    assert stats["out_classified"] == 0                    # nothing left in the work queue
    assert (stats["out_valid"], stats["out_rejected"], stats["out_duplicate"]) == (0, 0, 0)
    assert stats["out_flag_rows_updated"] == 0
    assert helpers.snapshot(db) == before                  # byte-for-byte same Silver


def test_incremental_run_classifies_only_new_rows_and_keeps_old_verdicts(loaded):
    db, ids = loaded
    run_silver.run(db)
    db.commit()
    before = helpers.verdicts(db)

    new_dup = helpers.insert_measurement(db, "N01_dup_of_V01", "1001", "2", "999", helpers.OK_UNIT,
                                         "2026-10-01T00:00:00Z", "2026-10-01T00:15:00Z")
    new_ok = helpers.insert_measurement(db, "N02_new_valid", "1001", "2", "20", helpers.OK_UNIT,
                                        "2026-10-05T00:00:00Z", "2026-10-05T00:15:00Z")
    new_bad = helpers.insert_measurement(db, "N03_new_bad", "1001", "2", "-1", helpers.OK_UNIT,
                                         "2026-10-05T01:00:00Z", "2026-10-05T01:15:00Z")
    db.commit()
    stats = run_silver.run(db)
    db.commit()

    assert stats["out_classified"] == 3
    after = helpers.verdicts(db)
    assert all(after[b] == before[b] for b in before)                   # old verdicts untouched
    assert after[new_dup]["classification"] == "DUPLICATE"
    assert after[new_dup]["duplicate_of_bronze_id"] == ids["V01_valid"]  # collides with Silver's EXISTING row
    assert after[new_dup]["value_differs"] is True
    assert after[new_ok]["classification"] == "VALID"
    assert after[new_bad]["reason_code"] == "NEGATIVE_PM25"
    c = helpers.counts(db)
    assert c["bronze"] == c["valid"] + c["rejected"] + c["duplicate"]


def test_full_refresh_reproduces_the_same_silver(loaded):
    db, _ = loaded
    run_silver.run(db)
    db.commit()
    before = helpers.snapshot(db)

    stats = run_silver.run(db, full_refresh=True)
    db.commit()

    assert stats["full_refresh"] is True
    assert stats["out_classified"] == _expected_counts()["bronze"]      # everything re-derived
    assert helpers.snapshot(db) == before


# ---------------------------------------------------------------- one transaction

def test_run_is_atomic_failure_midway_leaves_no_partial_silver(loaded):
    """Sabotage the LAST write step (rejected_records). Earlier steps - sensors, measurements -
    must be rolled back with it."""
    db, _ = loaded
    db.execute("CREATE OR REPLACE FUNCTION silver._boom() RETURNS trigger LANGUAGE plpgsql AS "
               "$$ BEGIN RAISE EXCEPTION 'boom'; END $$")
    db.execute("CREATE TRIGGER trg_boom BEFORE INSERT ON silver.rejected_records "
               "FOR EACH ROW EXECUTE FUNCTION silver._boom()")
    db.commit()
    try:
        with pytest.raises(psycopg.errors.RaiseException, match="boom"):
            run_silver.run(db)
        db.rollback()
        c = helpers.counts(db)
        assert c["silver_rows"] == 0 and c["classified"] == 0 and c["rejected_rows"] == 0
        assert db.execute("SELECT count(*) FROM silver.sensors").fetchone()[0] == 0
    finally:
        db.rollback()
        db.execute("DROP TRIGGER IF EXISTS trg_boom ON silver.rejected_records")
        db.execute("DROP FUNCTION IF EXISTS silver._boom()")
        db.commit()

    run_silver.run(db)                                   # and a clean retry works
    db.commit()
    assert helpers.counts(db)["classified"] == _expected_counts()["bronze"]


def test_reconciliation_guard_fails_the_run_and_rolls_it_back(loaded):
    db, ids = loaded
    run_silver.run(db)
    db.commit()
    # Corrupt Silver: remove a VALID row that nothing depends on.
    db.execute("DELETE FROM silver.measurements WHERE bronze_id = %s", (ids["V02_zero_is_valid"],))
    db.commit()
    with pytest.raises(psycopg.errors.RaiseException, match="reconciliation failed"):
        run_silver.run(db)
    db.rollback()


# ---------------------------------------------------------------- sensor dimension

def test_sensor_dimension_latest_snapshot_wins_and_bad_rows_are_handled(db):
    helpers.insert_sensor(db, 1001, location_name="Renamed Loc")            # newer snapshot of 1001
    helpers.insert_sensor(db, 2001, parameter_id="10")                       # not PM2.5 -> excluded
    helpers.insert_sensor(db, 2002, latitude="abc")                          # bad lat -> NULL, row kept
    helpers.insert_sensor(db, 2003, latitude="95", longitude="200")          # out of range -> NULL
    helpers.insert_sensor(db, "not-a-number")                                # unusable id -> excluded
    db.commit()
    stats = run_silver.run(db)
    db.commit()

    rows = {r[0]: r for r in db.execute(
        "SELECT sensor_id, location_name, latitude, longitude, is_monitor, parameter_id "
        "FROM silver.sensors").fetchall()}
    assert rows[1001][1] == "Renamed Loc"
    assert 2001 not in rows
    assert rows[2002][2] is None and rows[2002][3] is not None
    assert rows[2003][2] is None and rows[2003][3] is None
    assert rows[1001][4] is True and rows[1001][5] == 2
    assert len(rows) == len(helpers.SENSOR_IDS) + 2                          # 13 fixture + 2002 + 2003
    assert stats["out_sensors_upserted"] == len(rows)

    # second run: nothing newer in Bronze -> nothing to update
    assert run_silver.run(db)["out_sensors_upserted"] == 0
    db.commit()
