"""Hard rules, rule priority, and duplicate handling - one parametrized test per fixture row.

The expected verdict for every row is written next to the row in tests/fixtures/dirty_bronze.csv,
so the CSV is the executable specification of the Silver rules.
"""
import pytest

from pipeline import run_silver
from tests import helpers

CASES = helpers.read_cases()


@pytest.fixture(scope="module")
def silver_result(test_db):
    """Load the dirty fixture once, run Silver once, share the verdicts across this module."""
    conn = helpers.connect_clean()
    helpers.insert_sensors(conn)
    ids = helpers.load_cases(conn)
    stats = run_silver.run(conn)
    conn.commit()
    yield dict(conn=conn, ids=ids, verdicts=helpers.verdicts(conn), stats=stats)
    conn.close()


@pytest.mark.parametrize("case", CASES, ids=[c["case_id"] for c in CASES])
def test_every_fixture_row_gets_its_expected_verdict(silver_result, case):
    got = silver_result["verdicts"][silver_result["ids"][case["case_id"]]]

    assert got["classification"] == case["expected_class"]
    assert got["reason_code"] == (case["expected_reason"] or None)

    if case["expected_class"] == "REJECTED":
        # every rule that fired is recorded, in priority order
        assert got["all_reason_codes"] == case["expected_all_reasons"].split("|")
        assert got["reason_detail"], "a rejection must record the offending raw value"
    else:
        assert got["all_reason_codes"] is None

    if case["expected_class"] == "DUPLICATE":
        assert got["duplicate_of_bronze_id"] == silver_result["ids"][case["expected_duplicate_of"]]
        assert got["value_differs"] == (case["expected_value_differs"] == "true")
    else:
        assert got["duplicate_of_bronze_id"] is None
        assert got["value_differs"] is None


def test_fixture_exercises_every_hard_rule():
    """Guard against a rule silently losing its test: each rule must be the PRIMARY reason of
    at least one fixture row."""
    primary = {c["expected_reason"] for c in CASES if c["expected_class"] == "REJECTED"}
    assert primary == set(helpers.HARD_RULES)


def test_all_three_verdicts_occur_in_fixture(silver_result):
    seen = {v["classification"] for v in silver_result["verdicts"].values()}
    assert seen == {"VALID", "REJECTED", "DUPLICATE"}


def test_rejected_row_does_not_claim_the_key(silver_result):
    """E01 (invalid) arrived BEFORE E02 on the same key. If dedupe ran before validation,
    E02 would be a duplicate of garbage."""
    ids, v = silver_result["ids"], silver_result["verdicts"]
    assert v[ids["E01_invalid_row_arrives_first"]]["classification"] == "REJECTED"
    assert v[ids["E02_valid_after_rejected_same_key"]]["classification"] == "VALID"


def test_duplicates_always_point_at_the_valid_row_not_at_each_other(silver_result):
    ids, v = silver_result["ids"], silver_result["verdicts"]
    for dup in ("D02_exact_repeat", "D03_changed_value", "D04_third_copy"):
        assert v[ids[dup]]["duplicate_of_bronze_id"] == ids["D01_original"]


def test_same_timestamp_on_different_sensors_is_not_a_duplicate(silver_result):
    ids, v = silver_result["ids"], silver_result["verdicts"]
    assert v[ids["F01_same_time_sensor_1001"]]["classification"] == "VALID"
    assert v[ids["F02_same_time_sensor_1002"]]["classification"] == "VALID"


def test_valid_row_is_typed_and_keeps_off_grid_seconds(silver_result):
    conn, ids = silver_result["conn"], silver_result["ids"]
    row = conn.execute(
        "SELECT sensor_id, parameter_id, pm25_ugm3, period_from_utc, period_to_utc, duration_seconds "
        "FROM silver.measurements WHERE bronze_id = %s", (ids["V03_off_grid_timestamp"],)).fetchone()
    assert row[0] == 1001 and row[1] == 2
    assert str(row[2]) == "33.25"
    assert row[3].isoformat() == "2026-10-01T00:30:34+00:00"      # seconds preserved, not snapped
    assert row[4].isoformat() == "2026-10-01T00:45:34+00:00"
    assert row[5] == 900


def test_zero_is_a_valid_reading(silver_result):
    conn, ids = silver_result["conn"], silver_result["ids"]
    assert conn.execute("SELECT pm25_ugm3 FROM silver.measurements WHERE bronze_id=%s",
                        (ids["V02_zero_is_valid"],)).fetchone()[0] == 0


def test_database_enforces_the_natural_key(silver_result):
    """Belt and braces: even a buggy run could not store two rows for one key."""
    import psycopg
    conn = silver_result["conn"]
    with pytest.raises(psycopg.errors.UniqueViolation):
        conn.execute(
            "INSERT INTO silver.measurements (bronze_id, sensor_id, parameter_id, period_from_utc, "
            "period_to_utc, pm25_ugm3, silver_run_id) SELECT bronze_id, 1002, 2, '2026-10-02T00:00:00Z', "
            "'2026-10-02T00:15:00Z', 1, gen_random_uuid() FROM bronze.measurements "
            "WHERE bronze_id NOT IN (SELECT bronze_id FROM silver.measurements) LIMIT 1")
    conn.rollback()
