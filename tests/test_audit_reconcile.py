"""audit.reconcile() on its own: it re-verifies a finished run, and every family of check fails when - and
only when - the layer it guards is corrupted."""
import psycopg
import pytest

from pipeline import audit
from tests import helpers
from tests.audit_helpers import fleet, go, one, q, rows
from tests.fake_openaq import item


@pytest.fixture
def pdb(test_db):
    conn = helpers.connect_clean()
    conn.autocommit = True
    yield conn
    conn.close()


@pytest.fixture
def good_run(pdb):
    locations, sensors = fleet()
    sensors[101] = sensors[101] + [item("2026-10-02T03:00:00Z", 15, 99.0)]    # same key, other value -> DUPLICATE
    result, _, _ = go((locations, sensors))
    assert result["status"] == "SUCCESS"
    return result["run_id"]


def reconcile(pdb, run_id):
    return {c["check_name"]: c for c in audit.reconcile(pdb, run_id)}


def failing(pdb, run_id):
    return {n for n, c in reconcile(pdb, run_id).items() if c["status"] == "FAIL"}


def test_a_finished_run_can_be_reverified_later_from_its_stored_counts(pdb, good_run):
    res = reconcile(pdb, good_run)
    assert len(res) == 25 and failing(pdb, good_run) == set()
    assert res["bronze_equals_valid_rejected_duplicate"]["details"] == "VALID=8 REJECTED=0 DUPLICATE=1"
    assert res["bronze_equals_valid_rejected_duplicate"]["expected_value"] == 9 == res["bronze_equals_valid_rejected_duplicate"]["actual_value"]


def test_reconcile_writes_nothing_by_itself(pdb, good_run):
    before = one(pdb, "SELECT count(*) FROM audit.reconciliation_checks")
    reconcile(pdb, good_run)
    assert one(pdb, "SELECT count(*) FROM audit.reconciliation_checks") == before


def test_bronze_row_without_a_verdict_is_caught(pdb, good_run):
    pdb.execute("INSERT INTO bronze.measurements (sensor_id, parameter_id, value_raw, unit_raw, period_from_raw, period_to_raw, "
                "raw_json, source_url, run_id, row_hash) VALUES ('101','2','5','x','2026-10-02T09:00:00Z','2026-10-02T09:15:00Z', "
                "'{}', 'x', gen_random_uuid(), 'orphan-bronze-row')")
    assert {"bronze_equals_classified", "bronze_equals_valid_rejected_duplicate",
            "bronze_rows_without_verdict"} <= failing(pdb, good_run)


def test_silver_row_missing_is_caught_at_silver_and_at_every_gold_layer(pdb, good_run):
    pdb.execute("DELETE FROM silver.measurements WHERE bronze_id = (SELECT max(bronze_id) FROM silver.measurements)")
    f = failing(pdb, good_run)
    assert {"silver_measurements_equal_valid", "gold_hourly_equals_silver_readings",
            "gold_daily_equals_silver_readings", "gold_health_equals_silver_readings"} <= f


def test_a_changed_silver_value_is_caught_against_its_bronze_source(pdb, good_run):
    pdb.execute("UPDATE silver.measurements SET pm25_ugm3 = pm25_ugm3 + 1 WHERE bronze_id = (SELECT min(bronze_id) FROM silver.measurements)")
    assert failing(pdb, good_run) == {"silver_values_match_bronze"}


def test_a_duplicate_pointing_at_a_later_row_is_caught(pdb, good_run):
    later = one(pdb, "SELECT max(bronze_id) FROM silver.measurements")
    dup = one(pdb, "SELECT bronze_id FROM silver.bronze_classification WHERE classification='DUPLICATE'")
    pdb.execute("UPDATE silver.bronze_classification SET duplicate_of_bronze_id = %s WHERE bronze_id = %s", (later, dup))
    assert failing(pdb, good_run) == {"duplicates_point_at_earlier_valid_row"}


def test_gold_count_and_flag_corruption_is_caught(pdb, good_run):
    pdb.execute("UPDATE gold.hourly_sensor_pm25 SET n_readings = n_readings + 1, n_flatline_readings = 5, "
                "n_high_value_readings = 5 WHERE ctid = (SELECT ctid FROM gold.hourly_sensor_pm25 LIMIT 1)")
    assert {"gold_hourly_equals_silver_readings", "gold_flatline_flags_reconcile",
            "gold_high_value_flags_reconcile"} <= failing(pdb, good_run)


def test_a_missing_health_row_is_caught(pdb, good_run):
    pdb.execute("DELETE FROM gold.sensor_health WHERE ctid = (SELECT ctid FROM gold.sensor_health WHERE received_readings = 0 LIMIT 1)")
    # also visible as "this run recorded writing a row that is no longer there"
    assert failing(pdb, good_run) == {"gold_health_rows_complete", "run_gold_rows_carry_run_id"}


def test_out_of_range_coverage_and_empty_hours_are_caught_even_without_check_constraints(pdb, good_run):
    pdb.execute("ALTER TABLE gold.hourly_sensor_pm25 DROP CONSTRAINT ck_hourly_coverage, DROP CONSTRAINT ck_hourly_n")
    try:
        pdb.execute("UPDATE gold.hourly_sensor_pm25 SET coverage_pct = 150, n_readings = 0 "
                    "WHERE ctid = (SELECT ctid FROM gold.hourly_sensor_pm25 LIMIT 1)")
        f = failing(pdb, good_run)
        assert {"gold_coverage_within_0_100", "gold_hourly_rows_have_readings"} <= f
    finally:
        pdb.execute("DELETE FROM gold.hourly_sensor_pm25 WHERE coverage_pct = 150")
        pdb.execute("ALTER TABLE gold.hourly_sensor_pm25 ADD CONSTRAINT ck_hourly_coverage CHECK (coverage_pct BETWEEN 0 AND 100), "
                    "ADD CONSTRAINT ck_hourly_n CHECK (n_readings >= 1)")


def test_disabling_bronze_immutability_is_caught(pdb, good_run):
    pdb.execute("ALTER TABLE bronze.measurements DISABLE TRIGGER trg_measurements_immutable")
    try:
        res = reconcile(pdb, good_run)["bronze_immutability_triggers_enabled"]
        assert (res["status"], res["expected_value"], res["actual_value"]) == ("FAIL", 2, 1)
    finally:
        pdb.execute("ALTER TABLE bronze.measurements ENABLE TRIGGER trg_measurements_immutable")
    assert failing(pdb, good_run) == set()


def test_bronze_is_in_fact_immutable_in_this_database(pdb, good_run):
    with pytest.raises(psycopg.errors.RaiseException, match="immutable"):
        pdb.execute("UPDATE bronze.measurements SET value_raw = '0'")
    with pytest.raises(psycopg.errors.RaiseException, match="immutable"):
        pdb.execute("DELETE FROM bronze.sensors")


def test_run_level_attribution_mismatches_are_caught(pdb, good_run):
    pdb.execute("UPDATE audit.pipeline_runs SET bronze_rows_inserted = bronze_rows_inserted + 1, "
                "gold_rows_written = gold_rows_written + 1 WHERE run_id = %s", (good_run,))
    assert {"run_bronze_fetched_equals_inserted_plus_skipped", "run_bronze_rows_carry_run_id",
            "run_gold_rows_carry_run_id"} <= failing(pdb, good_run)


def test_a_sensor_without_a_fetch_status_row_is_caught(pdb, good_run):
    pdb.execute("DELETE FROM audit.sensor_fetch_status WHERE sensor_id = 107")
    assert failing(pdb, good_run) == {"run_sensors_accounted_for"}


def test_unknown_run_id_is_an_error_not_a_pass(pdb):
    with pytest.raises(psycopg.errors.RaiseException, match="unknown run_id"):
        pdb.execute("SELECT * FROM audit.reconcile(gen_random_uuid())")
