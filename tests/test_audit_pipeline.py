"""run_all end to end against a scripted fake API: audit trail, window, run_id flow, idempotency,
the A-E 'why is there no data' distinctions, failure handling, concurrency and secrets."""
import logging
import uuid
from datetime import date, datetime, timezone

import psycopg
import pytest
import requests

from pipeline import ingest_bronze, run_all
from pipeline.audit import PipelineBusy, PipelineFailed
from tests import helpers
from tests.audit_helpers import (IDS, KEY, UTC, W0, W1, checks, fleet, go, install_trigger, layer_counts, one, q,
                                 remove_trigger, rows, run_row)
from tests.fake_openaq import FakeSession, item, loc


@pytest.fixture
def pdb(test_db):
    conn = helpers.connect_clean()
    conn.autocommit = True
    yield conn
    conn.close()


# ---------------------------------------------------------------- success path

def test_successful_run_end_to_end(pdb):
    r, _, _ = go()
    assert (r["status"], r["stage"], r["error_message"]) == ("SUCCESS", "done", None)
    assert r["finished_at"] is not None and r["duration_seconds"] >= 0
    assert (r["requested_sensor_count"], r["skipped_inactive_count"], r["attempted_sensor_count"],
            r["successful_sensor_count"], r["failed_sensor_count"]) == (9, 1, 7, 6, 1)
    assert (r["bronze_rows_fetched"], r["bronze_rows_inserted"], r["bronze_rows_skipped"]) == (8, 8, 0)
    assert (r["silver_valid"], r["silver_rejected"], r["silver_duplicate"]) == (8, 0, 0)
    assert (r["gold_hourly_rows"], r["gold_daily_rows"], r["gold_health_rows"]) == (8, 8, 36)
    assert r["gold_rows_written"] == 8 + 8 + 36 and r["gold_rows_deleted"] == 0
    assert r["checks"] == {"PASS": 24, "WARN": 1}                         # the 1 warning is the tolerated fetch failure
    assert one(pdb, "SELECT status FROM audit.reconciliation_checks WHERE check_name='run_fetch_failure_rate'") == "WARN"


def test_the_run_row_is_RUNNING_while_the_pipeline_executes(pdb):
    seen = {}
    real = ingest_bronze.run_window

    def spy(conn, client, run_id, wf, wt, **kw):
        seen["row"] = rows(pdb, "SELECT status, stage, finished_at, window_from_utc, window_to_utc "
                                "FROM audit.pipeline_runs WHERE run_id = %s", (run_id,))[0]
        return real(conn, client, run_id, wf, wt, **kw)

    r, _, _ = go(ingest_fn=spy)
    assert (seen["row"]["status"], seen["row"]["stage"], seen["row"]["finished_at"]) == ("RUNNING", "bronze", None)
    assert (seen["row"]["window_from_utc"], seen["row"]["window_to_utc"]) == (W0, W1)
    assert r["status"] == "SUCCESS"


# ---------------------------------------------------------------- the true window

def test_requested_window_is_stored_exactly_and_gold_measures_coverage_against_it(pdb):
    r, _, _ = go()
    assert (r["window_from_utc"], r["window_to_utc"]) == (W0, W1)
    assert (r["effective_horizon_from_utc"], r["effective_horizon_to_utc"]) == (W0, W1)
    # Window start 2026-10-02T00:00Z is 05:30 IST, so the first local day expects 18.5 h = 66,600 s.
    # A horizon inferred from the data (first reading 03:00Z = 08:30 IST) would have said 55,800.
    h = one(pdb, "SELECT expected_seconds FROM gold.sensor_health WHERE sensor_id = 101 AND local_date = '2026-10-02'")
    assert h == 66600


def test_periods_overlapping_the_window_edge_widen_only_the_effective_horizon(pdb):
    locations, sensors = fleet()
    sensors[101] = sensors[101] + [item("2026-10-01T23:30:00Z", 60, 9)]      # starts 30 min BEFORE the window
    r, _, _ = go((locations, sensors))
    assert r["status"] == "SUCCESS"
    assert r["window_from_utc"] == W0                                         # what was asked for, untouched
    assert r["effective_horizon_from_utc"] == datetime(2026, 10, 1, 23, 30, tzinfo=UTC)   # what Gold had to cover
    assert r["observed_from_utc"] == datetime(2026, 10, 1, 23, 30, tzinfo=UTC)            # what the API actually returned
    assert r["observed_to_utc"] == datetime(2026, 10, 4, 3, 15, tzinfo=UTC)


# ---------------------------------------------------------------- one run_id through every layer

def test_the_same_run_id_flows_through_every_layer(pdb):
    r, _, _ = go()
    rid = r["run_id"]
    n = lambda sql: one(pdb, sql, (rid,))
    assert n("SELECT count(*) FROM bronze.measurements WHERE run_id = %s") == 8
    assert one(pdb, "SELECT count(DISTINCT run_id)::int FROM bronze.measurements") == 1
    assert n("SELECT count(*) FROM bronze.sensors WHERE run_id = %s") == 9
    assert n("SELECT count(*) FROM silver.bronze_classification WHERE silver_run_id = %s") == 8
    assert n("SELECT count(*) FROM silver.measurements WHERE silver_run_id = %s") == 8
    for t, total in (("hourly_sensor_pm25", 8), ("daily_location_summary", 8), ("sensor_health", 36)):
        assert n(f"SELECT count(*) FROM gold.{t} WHERE gold_run_id = %s") == total
    assert n("SELECT count(*) FROM audit.fetch_attempts WHERE run_id = %s") > 0
    assert n("SELECT count(*) FROM audit.sensor_fetch_status WHERE run_id = %s") == 9
    assert n("SELECT count(*) FROM audit.reconciliation_checks WHERE run_id = %s") == 25
    assert one(pdb, "SELECT count(*) FROM audit.pipeline_runs") == 1


# ---------------------------------------------------------------- idempotency

def test_an_identical_second_run_changes_nothing_but_is_fully_audited(pdb):
    first, _, _ = go()
    before = layer_counts(pdb)
    second, _, _ = go()
    assert second["run_id"] != first["run_id"] and second["status"] == "SUCCESS"
    assert (second["bronze_rows_fetched"], second["bronze_rows_inserted"], second["bronze_rows_skipped"]) == (8, 0, 8)
    assert (second["silver_valid"], second["silver_rejected"], second["silver_duplicate"]) == (0, 0, 0)
    assert second["gold_rows_written"] == 0 and second["gold_rows_deleted"] == 0
    assert layer_counts(pdb) == before
    assert second["checks"] == {"PASS": 24, "WARN": 1}
    # the first run's rows still carry the first run's id (a no-op run does not rewrite lineage)
    assert one(pdb, "SELECT count(DISTINCT gold_run_id)::int FROM gold.hourly_sensor_pm25") == 1
    assert one(pdb, "SELECT count(*) FROM audit.pipeline_runs WHERE status = 'SUCCESS'") == 2


# ---------------------------------------------------------------- fetch audit

def test_every_http_attempt_is_recorded_including_retries_and_the_discovery_call(pdb):
    r, _, sleeps = go()
    rid = r["run_id"]
    disc = rows(pdb, "SELECT * FROM audit.fetch_attempts WHERE run_id=%s AND endpoint='locations'", (rid,))
    assert len(disc) == 1 and disc[0]["sensor_id"] is None and disc[0]["rows_returned"] == 9 and disc[0]["outcome"] == "SUCCESS"

    bad = rows(pdb, "SELECT * FROM audit.fetch_attempts WHERE run_id=%s AND sensor_id=105 ORDER BY attempt_no", (rid,))
    assert [a["attempt_no"] for a in bad] == [1, 2, 3, 4, 5]
    assert {(a["outcome"], a["http_status"]) for a in bad} == {("HTTP_5XX", 500)}
    assert [a["backoff_seconds"] for a in bad] == [2, 4, 8, 16, None]
    assert [a["is_final"] for a in bad] == [False] * 4 + [True]
    assert (bad[0]["request_window_from"], bad[0]["request_window_to"]) == (W0, W1)
    assert "sensors/105/measurements" in bad[0]["request_url"] and bad[0]["parameter_id"] == 2
    assert sleeps.count(2) == 1 and sleeps.count(16) == 1                       # the fake clock, not the wall clock

    ok = rows(pdb, "SELECT * FROM audit.fetch_attempts WHERE run_id=%s AND sensor_id=101", (rid,))
    assert [(a["outcome"], a["rows_returned"], a["attempt_no"]) for a in ok] == [("SUCCESS", 3, 1)]
    for sid in (106, 107):                                                       # skipped / excluded: never requested
        assert one(pdb, "SELECT count(*) FROM audit.fetch_attempts WHERE run_id=%s AND sensor_id=%s", (rid, sid)) == 0


def test_sensor_fetch_status_accounts_for_every_discovered_sensor(pdb):
    r, _, _ = go()
    st = {x["sensor_id"]: x for x in rows(pdb, "SELECT * FROM audit.sensor_fetch_status WHERE run_id=%s", (r["run_id"],))}
    assert {k: v["status"] for k, v in st.items()} == {
        101: "FETCHED", 102: "FETCHED", 103: "FETCHED", 201: "FETCHED", 202: "FETCHED",
        104: "ZERO_ROWS", 105: "FAILED", 106: "SKIPPED_INACTIVE", 107: "NOT_ATTEMPTED"}
    assert (st[105]["last_http_status"], st[105]["error_type"], st[105]["http_attempts"]) == (500, "HTTP_5XX", 5)
    assert (st[101]["rows_fetched"], st[101]["pages"], st[101]["http_attempts"]) == (3, 1, 1)
    assert st[101]["first_period_from_utc"] == datetime(2026, 10, 2, 3, 0, tzinfo=UTC)
    assert st[101]["last_period_to_utc"] == datetime(2026, 10, 4, 3, 15, tzinfo=UTC)
    assert "before the window start" in st[106]["reason"] and "excluded" in st[107]["reason"]
    assert st[104]["rows_fetched"] == 0 and st[104]["last_http_status"] == 200


def test_no_data_has_five_distinct_explanations(pdb):
    """A zero rows / B request failed / C never attempted / D skipped inactive / E provider stopped."""
    go()
    why = {(r["sensor_id"], r["local_date"]): r["no_data_reason"] for r in rows(pdb, "SELECT * FROM audit.v_no_data_reason")}
    d3, d4, d5 = date(2026, 10, 3), date(2026, 10, 4), date(2026, 10, 5)
    assert why[(104, d3)] == "SOURCE_RETURNED_ZERO_ROWS"          # A
    assert why[(105, d3)] == "FETCH_FAILED"                       # B
    assert why[(107, d3)] == "NOT_ATTEMPTED"                      # C
    assert why[(106, d3)] == "SKIPPED_INACTIVE"                   # D
    assert why[(201, d3)] == "PROVIDER_STOPPED_DELIVERING"        # E: both PROV2 sensors stop after 10-02
    assert why[(101, d5)] == "PROVIDER_STOPPED_DELIVERING"        # E: CPCB's newest data is 10-04 03:15Z
    assert why[(102, d4)] == "SENSOR_STOPPED_DELIVERING"          # this sensor ended, its provider did not
    assert why[(103, d3)] == "GAP_WITHIN_SENSOR_DATA"             # data before and after
    # ...while Gold alone calls every one of them plain NO_DATA
    gold = {(r[0], r[1]): r[2] for r in q(pdb, "SELECT sensor_id, local_date, health_status FROM gold.sensor_health")}
    assert all(gold[k] == "NO_DATA" for k in why)


def test_provider_freshness_shows_how_far_behind_each_provider_is(pdb):
    r, _, _ = go()
    fresh = {x["provider_name"]: x for x in rows(pdb, "SELECT * FROM audit.v_provider_freshness WHERE run_id=%s", (r["run_id"],))}
    assert fresh["CPCB"]["last_data_utc"] == datetime(2026, 10, 4, 3, 15, tzinfo=UTC)
    assert float(fresh["CPCB"]["hours_behind_window_end"]) == 20.8
    assert float(fresh["PROV2"]["hours_behind_window_end"]) == 68.8
    assert (fresh["CPCB"]["failed"], fresh["CPCB"]["zero_rows"], fresh["CPCB"]["skipped_inactive"],
            fresh["CPCB"]["not_attempted"]) == (1, 1, 1, 1)


def test_fifteen_failed_sensors_are_distinguishable_from_silent_ones(pdb):
    locations, sensors = [], {}
    for sid in range(301, 316):                                   # 15 reproducible HTTP 500s
        locations.append(loc(sid, [sid])); sensors[sid] = 500
    for sid in range(316, 331):                                   # 15 sensors that genuinely have nothing
        locations.append(loc(sid, [sid])); sensors[sid] = []
    for sid in (340, 341, 342):
        locations.append(loc(sid, [sid])); sensors[sid] = [item("2026-10-03T03:00:00Z", 15, 10)]
    r, _, _ = go((locations, sensors), sensor_ids=None, max_failed_fraction=1.0)
    assert r["status"] == "SUCCESS"
    st = {k: v for k, v in q(pdb, "SELECT status, count(*) FROM audit.sensor_fetch_status WHERE run_id=%s GROUP BY 1", (r["run_id"],))}
    assert st == {"FAILED": 15, "ZERO_ROWS": 15, "FETCHED": 3}
    d3 = date(2026, 10, 3)
    gold_status = {s: h for s, h in q(pdb, "SELECT sensor_id, health_status FROM gold.sensor_health WHERE local_date=%s", (d3,))}
    assert all(gold_status[s] == "NO_DATA" for s in range(301, 331))             # Gold cannot tell them apart...
    why = {s: w for s, w in q(pdb, "SELECT sensor_id, no_data_reason FROM audit.v_no_data_reason WHERE local_date=%s", (d3,))}
    assert [why[s] for s in range(301, 316)] == ["FETCH_FAILED"] * 15            # ...the audit trail can
    assert [why[s] for s in range(316, 331)] == ["SOURCE_RETURNED_ZERO_ROWS"] * 15


# ---------------------------------------------------------------- failure policy and failure handling

def assert_failed(exc_info, pdb, stage, *, rolled_back=True, check=None, message=None):
    """Common post-conditions of ANY failed run: never SUCCESS, finished, explained, evidence kept."""
    r = exc_info.value.result
    assert one(pdb, "SELECT count(*) FROM audit.pipeline_runs WHERE status = 'SUCCESS'") == 0       # no false SUCCESS
    assert one(pdb, "SELECT count(*) FROM audit.pipeline_runs WHERE status = 'RUNNING'") == 0       # nothing left dangling
    run = run_row(pdb, r["run_id"])
    assert (run["status"], run["stage"]) == ("FAILED", stage)
    assert run["finished_at"] is not None and run["error_message"]
    assert run["silver_valid"] is None and run["gold_hourly_rows"] is None         # figures of a rolled-back transaction are not reported
    if message:
        assert message in run["error_message"]
    if check:
        assert checks(pdb, r["run_id"])[check]["status"] == "FAIL"
    if rolled_back:
        c = layer_counts(pdb)
        assert (c["silver"], c["gold"]) == (0, 0) and c["bronze"] == 8             # Silver+Gold undone, Bronze (raw) kept
    return run


def test_failure_rate_above_tolerance_fails_the_run_and_rolls_back_silver_and_gold(pdb):
    with pytest.raises(PipelineFailed) as e:
        go(max_failed_fraction=0.05)                              # 1 of 7 attempted = 14.3%
    run = assert_failed(e, pdb, "reconcile", check="run_fetch_failure_rate", message="run_fetch_failure_rate")
    assert "105:500" in one(pdb, "SELECT details FROM audit.reconciliation_checks WHERE check_name='run_fetch_failure_rate'")
    assert one(pdb, "SELECT count(*) FROM audit.fetch_attempts WHERE run_id=%s AND sensor_id=105", (run["run_id"],)) == 5


def test_bronze_accounting_failure_fails_the_run(pdb):
    real = ingest_bronze.run_window

    def lying(conn, client, run_id, wf, wt, **kw):
        stats = real(conn, client, run_id, wf, wt, **kw)
        stats["meas_rows_fetched"] += 1                           # claims one row that was never inserted or skipped
        return stats

    with pytest.raises(PipelineFailed) as e:
        go(ingest_fn=lying, max_failed_fraction=1.0)
    assert_failed(e, pdb, "reconcile", check="run_bronze_fetched_equals_inserted_plus_skipped")


def test_bronze_rows_written_under_another_run_id_fail_the_run(pdb):
    real = ingest_bronze.run_window

    def foreign(conn, client, run_id, wf, wt, **kw):
        return real(conn, client, uuid.uuid4(), wf, wt, **kw)    # lineage broken: rows carry a different run_id

    with pytest.raises(PipelineFailed) as e:
        go(ingest_fn=foreign, max_failed_fraction=1.0)
    assert_failed(e, pdb, "reconcile", check="run_bronze_rows_carry_run_id")


def test_silver_reconciliation_failure_fails_the_run(pdb):
    install_trigger(pdb, "silver.bronze_classification", "IF NEW.bronze_id % 2 = 0 THEN RETURN NULL; END IF;")
    try:
        with pytest.raises(PipelineFailed) as e:
            go(max_failed_fraction=1.0)
    finally:
        remove_trigger(pdb, "silver.bronze_classification")
    assert_failed(e, pdb, "silver", check="silver_guard", message="silver reconciliation failed")


def test_gold_reconciliation_failure_fails_the_run(pdb):
    install_trigger(pdb, "gold.hourly_sensor_pm25", "NEW.n_readings := NEW.n_readings + 1;")
    try:
        with pytest.raises(PipelineFailed) as e:
            go(max_failed_fraction=1.0)
    finally:
        remove_trigger(pdb, "gold.hourly_sensor_pm25")
    assert_failed(e, pdb, "gold", check="gold_guard_G1", message="G1 failed")


def test_an_unexpected_exception_fails_the_run_and_is_chained_not_swallowed(pdb):
    def boom(*a, **k):
        raise ValueError(f"disk on fire (key {KEY})")

    with pytest.raises(PipelineFailed) as e:
        go(ingest_fn=boom)
    assert isinstance(e.value.__cause__, ValueError)
    run = run_row(pdb, e.value.result["run_id"])
    assert (run["status"], run["stage"]) == ("FAILED", "bronze") and "ValueError" in run["error_message"]
    assert KEY not in run["error_message"]                                         # redacted even in failures


def test_ctrl_c_is_recorded_as_failed_but_still_propagates(pdb):
    def interrupt(*a, **k):
        raise KeyboardInterrupt()

    with pytest.raises(KeyboardInterrupt):
        go(ingest_fn=interrupt)
    (run,) = rows(pdb, "SELECT * FROM audit.pipeline_runs")
    assert run["status"] == "FAILED" and run["finished_at"] is not None and "KeyboardInterrupt" in run["error_message"]


def test_failed_runs_leave_all_their_audit_evidence_behind(pdb):
    with pytest.raises(PipelineFailed) as e:
        go(max_failed_fraction=0.05)
    rid = e.value.result["run_id"]
    assert one(pdb, "SELECT count(*) FROM audit.fetch_attempts WHERE run_id=%s", (rid,)) > 0
    assert one(pdb, "SELECT count(*) FROM audit.sensor_fetch_status WHERE run_id=%s", (rid,)) == 9
    assert one(pdb, "SELECT count(*) FROM audit.reconciliation_checks WHERE run_id=%s", (rid,)) == 25
    assert run_row(pdb, rid)["bronze_rows_inserted"] == 8                          # Bronze counts are real and kept
    r2, _, _ = go(max_failed_fraction=1.0)                                         # and the pipeline recovers on the next run
    assert r2["status"] == "SUCCESS" and r2["bronze_rows_inserted"] == 0 and r2["silver_valid"] == 8


# ---------------------------------------------------------------- concurrency and constraints

def test_a_second_concurrent_run_is_refused_without_touching_anything(pdb, test_db):
    from pipeline import config
    other = psycopg.connect(**config.db_kwargs(test_db), autocommit=True)
    try:
        other.execute("SELECT pg_advisory_lock(hashtext('audit.run_all'))")
        with pytest.raises(PipelineBusy):
            go()
    finally:
        other.execute("SELECT pg_advisory_unlock_all()")
        other.close()
    assert one(pdb, "SELECT count(*) FROM audit.pipeline_runs") == 0
    assert layer_counts(pdb) == dict(bronze=0, silver=0, gold=0)
    assert go()[0]["status"] == "SUCCESS"                                          # fine once the lock is free


def test_a_crashed_run_left_RUNNING_is_marked_abandoned_by_the_next_run(pdb):
    orphan = uuid.uuid4()
    pdb.execute("INSERT INTO audit.pipeline_runs (run_id, status, window_from_utc, window_to_utc) "
                "VALUES (%s, 'RUNNING', %s, %s)", (orphan, W0, W1))
    r, _, _ = go()
    old = run_row(pdb, orphan)
    assert old["status"] == "FAILED" and "abandoned" in old["error_message"] and old["finished_at"] is not None
    assert r["status"] == "SUCCESS" and r["parameters"]["abandoned_runs_cleaned"] == 1


def test_the_database_itself_allows_only_one_RUNNING_run(pdb):
    ins = ("INSERT INTO audit.pipeline_runs (run_id, status, window_from_utc, window_to_utc) "
           "VALUES (gen_random_uuid(), 'RUNNING', %s, %s)")
    pdb.execute(ins, (W0, W1))
    with pytest.raises(psycopg.errors.UniqueViolation):
        pdb.execute(ins, (W0, W1))


@pytest.mark.parametrize("status, stage, finished, error", [
    ("SUCCESS", "start", True, None),     # SUCCESS must be at stage 'done'
    ("SUCCESS", "done", False, None),     # SUCCESS must have finished
    ("RUNNING", "start", True, None),     # RUNNING must not have finished
    ("FAILED", "bronze", True, None),     # FAILED must explain itself
    ("SUCCESS", "done", True, "oops"),    # SUCCESS must not carry an error
], ids=["success-not-done", "success-no-finish", "running-finished", "failed-no-error", "success-with-error"])
def test_inconsistent_run_states_are_impossible(pdb, status, stage, finished, error):
    with pytest.raises(psycopg.errors.CheckViolation):
        pdb.execute("INSERT INTO audit.pipeline_runs (run_id, status, stage, finished_at, error_message, "
                    "window_from_utc, window_to_utc) VALUES (gen_random_uuid(), %s, %s, "
                    "CASE WHEN %s THEN now() END, %s, %s, %s)", (status, stage, finished, error, W0, W1))


def test_a_window_must_be_ordered(pdb):
    with pytest.raises(psycopg.errors.CheckViolation):
        pdb.execute("INSERT INTO audit.pipeline_runs (run_id, status, window_from_utc, window_to_utc) "
                    "VALUES (gen_random_uuid(), 'RUNNING', %s, %s)", (W1, W0))


# ---------------------------------------------------------------- secrets

def test_the_api_key_never_reaches_any_audit_table_or_log(pdb, caplog):
    caplog.set_level(logging.DEBUG)
    locations, sensors = fleet()
    sensors[105] = requests.ConnectionError(f"proxy rejected header X-API-Key: {KEY}")
    r, session, _ = go((locations, sensors))
    assert r["status"] == "SUCCESS"
    assert session.headers["X-API-Key"] == KEY                                     # it was really used...
    for table in ("pipeline_runs", "fetch_attempts", "sensor_fetch_status", "reconciliation_checks"):
        assert one(pdb, f"SELECT count(*) FROM audit.{table} t WHERE t::text LIKE %s", (f"%{KEY}%",)) == 0, table
    assert KEY not in caplog.text
    msg = one(pdb, "SELECT error_message FROM audit.fetch_attempts WHERE sensor_id=105 AND attempt_no=1")
    assert "REDACTED" in msg and "ConnectionError" in one(pdb, "SELECT error_type FROM audit.fetch_attempts WHERE sensor_id=105 AND attempt_no=1")


# ---------------------------------------------------------------- gaps found while planning the mutation check

def test_a_critical_check_failing_at_pipeline_level_fails_and_rolls_back_the_run(pdb):
    """Severity matters: with the Bronze immutability triggers off, the run must FAIL, not just report it."""
    pdb.execute("ALTER TABLE bronze.measurements DISABLE TRIGGER trg_measurements_immutable")
    try:
        with pytest.raises(PipelineFailed) as e:
            go(max_failed_fraction=1.0)
    finally:
        pdb.execute("ALTER TABLE bronze.measurements ENABLE TRIGGER trg_measurements_immutable")
    assert_failed(e, pdb, "reconcile", check="bronze_immutability_triggers_enabled")
    assert checks(pdb, e.value.result["run_id"])["bronze_immutability_triggers_enabled"]["severity"] == "CRITICAL"


def test_a_database_error_inside_ingest_fails_the_run_instead_of_being_counted_as_a_sensor_failure(pdb):
    """Only FetchError is a per-sensor failure. A DB error is a pipeline failure and must propagate."""
    install_trigger(pdb, "bronze.measurements", "RAISE EXCEPTION 'db unavailable'; ")
    try:
        with pytest.raises(PipelineFailed) as e:
            go(max_failed_fraction=1.0)
    finally:
        remove_trigger(pdb, "bronze.measurements")
    run = run_row(pdb, e.value.result["run_id"])
    assert (run["status"], run["stage"]) == ("FAILED", "bronze") and "db unavailable" in run["error_message"]
    assert isinstance(e.value.__cause__, psycopg.errors.RaiseException)
    assert one(pdb, "SELECT count(*) FROM audit.sensor_fetch_status WHERE run_id=%s AND status='FAILED'", (run["run_id"],)) == 0


def test_bronze_immutability_still_blocks_updates_with_audit_in_place(pdb):
    go(max_failed_fraction=1.0)
    with pytest.raises(psycopg.errors.RaiseException, match="immutable"):
        pdb.execute("DELETE FROM bronze.measurements")


def test_a_lone_sensor_stopping_is_not_called_a_provider_outage(pdb):
    """'Provider stopped' needs at least two data-bearing sensors of that provider to have stopped; one sensor
    ending is a SENSOR_STOPPED_DELIVERING."""
    locations, sensors = fleet()
    locations.append(loc(10, [401], provider="SOLO"))
    sensors[401] = [item("2026-10-02T03:00:00Z", 15, 50)]
    r, _, _ = go((locations, sensors), sensor_ids=IDS + ["401"])
    why = {(s, d): w for s, d, w in q(pdb, "SELECT sensor_id, local_date, no_data_reason FROM audit.v_no_data_reason")}
    assert why[(401, date(2026, 10, 3))] == "SENSOR_STOPPED_DELIVERING"
    assert why[(201, date(2026, 10, 3))] == "PROVIDER_STOPPED_DELIVERING"          # two PROV2 sensors: provider-wide
