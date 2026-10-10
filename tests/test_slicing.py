"""30-day time slicing of the per-sensor fetch window.

Why it exists: the API paginates by offset, so deep pages of a long window got slow and then timed out (HTTP 408) in the
90-day run. Fetching each sensor in slices of <= 30 days keeps every request shallow. The tests use the fake API; none
of them touches the network."""
import random
from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from pipeline.audit import PipelineFailed
from pipeline.ingest_bronze import SLICE_OVERLAP, time_slices
from tests import helpers
from tests.audit_helpers import go, one, q, rows, run_row
from tests.fake_openaq import item, loc

UTC = timezone.utc
SW0 = datetime(2026, 7, 11, tzinfo=UTC)
DAY = timedelta(days=1)
SW1 = SW0 + 65 * DAY                          # a 65-day window: slices of 30 + 30 + 5 days
B1, B2 = SW0 + 30 * DAY, SW0 + 60 * DAY       # the two interior slice boundaries
SEMANTICS = ["end_in", "start_in", "overlap", "end_exclusive", "start_exclusive"]


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


@pytest.fixture
def pdb(test_db):
    conn = helpers.connect_clean()
    conn.autocommit = True
    yield conn
    conn.close()


def run_sliced(sensors, *, semantics="end_in", window_to=SW1, **kw):
    """Run the whole pipeline over a 65-day window for the given {sensor_id: behaviour} fleet."""
    locations = [loc(sid - 100, [sid]) for sid in sensors]
    return go((locations, sensors), window_from=SW0, window_to=window_to, sensor_ids=None,
              semantics=semantics, **kw)


def boundary_items():
    """Measurements placed exactly on, just before and straddling both interior slice boundaries."""
    out, v = [item(iso(SW0 + 5 * DAY), 15, 1.0), item(iso(SW1 - DAY), 15, 2.0)], 3.0
    for b in (B1, B2):
        out += [item(iso(b - timedelta(minutes=15)), 15, v),         # ends exactly on the boundary
                item(iso(b), 15, v + 1),                             # starts exactly on the boundary
                item(iso(b - timedelta(minutes=10)), 30, v + 2)]     # straddles it
        v += 3
    return out


# ------------------------------------------------------------------ the slice arithmetic

def test_a_90_day_window_is_exactly_three_30_day_slices():
    sl = time_slices(SW0, SW0 + 90 * DAY)
    assert len(sl) == 3
    assert all(e - s == 30 * DAY for s, e in sl)
    assert (sl[0][0], sl[-1][1]) == (SW0, SW0 + 90 * DAY)
    assert all(sl[i][1] == sl[i + 1][0] for i in range(2))             # consecutive


@pytest.mark.parametrize("days", [1, 10, 29, 30])
def test_a_window_of_30_days_or_less_is_one_slice(days):
    assert time_slices(SW0, SW0 + days * DAY) == [(SW0, SW0 + days * DAY)]


def test_a_window_just_over_30_days_needs_a_second_slice():
    sl = time_slices(SW0, SW0 + 30 * DAY + timedelta(seconds=1))
    assert [e - s for s, e in sl] == [30 * DAY, timedelta(seconds=1)]


def test_a_65_day_window_is_30_plus_30_plus_5_days():
    sl = time_slices(SW0, SW1)
    assert [(e - s).days for s, e in sl] == [30, 30, 5]
    assert sl[1][0] == B1 and sl[2][0] == B2 and sl[-1][1] == SW1


def test_slices_always_cover_the_window_exactly_with_no_gap_or_overlap():
    rnd = random.Random(7)
    for _ in range(200):
        span = timedelta(seconds=rnd.randint(1, 200 * 86400))
        sl = time_slices(SW0, SW0 + span)
        assert sl[0][0] == SW0 and sl[-1][1] == SW0 + span
        assert all(timedelta(0) < e - s <= 30 * DAY for s, e in sl)
        assert all(sl[i][1] == sl[i + 1][0] for i in range(len(sl) - 1))


def test_an_empty_window_has_no_slices():
    assert time_slices(SW0, SW0) == []


# ------------------------------------------------------------------ boundaries: nothing lost, nothing duplicated

@pytest.mark.parametrize("semantics", SEMANTICS)
def test_no_measurement_is_lost_at_a_slice_boundary_whatever_the_apis_boundary_rule(pdb, semantics):
    items = boundary_items()
    r, _, _ = run_sliced({101: items}, semantics=semantics)
    assert r["status"] == "SUCCESS"
    got = {x[0] for x in q(pdb, "SELECT period_from_raw FROM bronze.measurements")}
    assert got == {it["period"]["datetimeFrom"]["utc"] for it in items}
    assert r["silver_valid"] == len(items)


def test_overlapping_slices_fetch_some_rows_twice_but_store_each_measurement_once(pdb):
    items = boundary_items()
    r, _, _ = run_sliced({101: items}, semantics="end_in")
    assert r["bronze_rows_inserted"] == len(items)
    assert r["bronze_rows_skipped"] == 2                                # the period ending exactly on each boundary
    assert r["bronze_rows_fetched"] == r["bronze_rows_inserted"] + r["bronze_rows_skipped"]
    assert one(pdb, "SELECT count(*) - count(DISTINCT (sensor_id, period_from_raw)) FROM bronze.measurements") == 0
    assert one(pdb, "SELECT count(*) FROM silver.bronze_classification WHERE classification = 'DUPLICATE'") == 0
    assert SLICE_OVERLAP == timedelta(hours=1)                          # documented, deliberate


def test_without_the_overlap_the_worst_case_apis_would_lose_boundary_rows():
    """Documents WHY the overlap exists: with 'end_exclusive' semantics the period ending exactly on a boundary is
    returned by neither slice if the slices merely abut."""
    items = boundary_items()
    abutting = time_slices(SW0, SW1)
    from tests.fake_openaq import in_window
    seen = set()
    for s, e in abutting:
        seen |= {it["period"]["datetimeFrom"]["utc"] for it in items if in_window(it, iso(s), iso(e), "end_exclusive")}
    assert len(seen) < len(items)                                       # rows really would be lost without overlap


# ------------------------------------------------------------------ pagination, retry, audit inside slices

def paging_fleet(first_attempt_status):
    """Sensor 101: 1,500 readings in slice 1 (two pages), 10 in slice 2, whose first request fails once."""
    start1 = SW0 + DAY
    items = [item(iso(start1 + timedelta(minutes=15 * i)), 15, 1 + (i % 50)) for i in range(1500)]
    items += [item(iso(B1 + DAY + timedelta(minutes=15 * i)), 15, 60 + i) for i in range(10)]
    state = {"failed": False}

    def behaviour(params):
        if params["datetime_from"] == iso(B1 - SLICE_OVERLAP) and not state["failed"]:
            state["failed"] = True
            return first_attempt_status
        return items

    return {101: behaviour}, len(items)


@pytest.mark.parametrize("status, outcome", [(500, "HTTP_5XX"), (408, "TIMEOUT")], ids=["500", "408"])
def test_each_slice_keeps_its_own_pagination_and_retries_a_transient_failure(pdb, status, outcome):
    fleet, n_items = paging_fleet(status)
    r, _, _ = run_sliced(fleet)
    assert r["status"] == "SUCCESS"
    att = rows(pdb, "SELECT request_window_from f, request_window_to t, page, attempt_no, outcome, http_status, rows_returned "
                    "FROM audit.fetch_attempts WHERE run_id=%s AND sensor_id=101 ORDER BY started_at", (r["run_id"],))
    shape = [(a["f"], a["t"], a["page"], a["attempt_no"], a["outcome"], a["http_status"], a["rows_returned"]) for a in att]
    assert shape == [
        (SW0, B1, 1, 1, "SUCCESS", 200, 1000),                      # slice 1, page 1
        (SW0, B1, 2, 1, "SUCCESS", 200, 500),                       # slice 1, page 2 (short page ends the slice)
        (B1 - SLICE_OVERLAP, B2, 1, 1, outcome, status, None),      # slice 2: transient failure ...
        (B1 - SLICE_OVERLAP, B2, 1, 2, "SUCCESS", 200, 10),         # ... retried within the slice
        (B2 - SLICE_OVERLAP, SW1, 1, 1, "SUCCESS", 200, 0),         # slice 3
    ]
    st = rows(pdb, "SELECT status, pages, http_attempts, rows_fetched FROM audit.sensor_fetch_status "
                   "WHERE run_id=%s AND sensor_id=101", (r["run_id"],))[0]
    assert (st["status"], st["pages"], st["http_attempts"], st["rows_fetched"]) == ("FETCHED", 4, 5, n_items)
    assert one(pdb, "SELECT count(*) FROM bronze.measurements") == n_items


def failing_slice_fleet():
    """Sensor 101 returns data for slice 1 and fails permanently (HTTP 500) for slice 2 onwards."""
    items = [item(iso(SW0 + DAY + timedelta(minutes=15 * i)), 15, 1 + i) for i in range(20)]
    items += [item(iso(B1 + DAY), 15, 99.0), item(iso(B2 + DAY), 15, 98.0)]

    def behaviour(params):
        return 500 if params["datetime_from"] >= iso(B1 - SLICE_OVERLAP) else items

    return {101: behaviour}


def test_a_permanently_failing_slice_is_recorded_precisely_and_later_slices_are_not_requested(pdb):
    r, _, sleeps = run_sliced(failing_slice_fleet(), max_failed_fraction=1.0)
    assert r["status"] == "SUCCESS" and r["failed_sensor_count"] == 1       # tolerated here; the rate is reconciled
    st = rows(pdb, "SELECT status, reason, last_http_status, error_type, rows_fetched, pages, http_attempts "
                   "FROM audit.sensor_fetch_status WHERE run_id=%s AND sensor_id=101", (r["run_id"],))[0]
    assert st["status"] == "FAILED" and st["error_type"] == "HTTP_5XX" and st["last_http_status"] == 500
    assert "[slice 2026-08-09T23:00:00Z -> 2026-09-09T00:00:00Z]" in st["reason"]       # names the failing slice
    assert st["rows_fetched"] == 20                                                       # slice 1 was kept
    att = rows(pdb, "SELECT request_window_from f, attempt_no, outcome FROM audit.fetch_attempts "
                    "WHERE run_id=%s AND sensor_id=101 ORDER BY started_at", (r["run_id"],))
    bad = [a for a in att if a["f"] == B1 - SLICE_OVERLAP]
    assert [a["attempt_no"] for a in bad] == [1, 2, 3, 4, 5] and {a["outcome"] for a in bad} == {"HTTP_5XX"}
    assert not [a for a in att if a["f"] == B2 - SLICE_OVERLAP]                           # fail fast: slice 3 never asked
    assert one(pdb, "SELECT count(*) FROM bronze.measurements") == 20
    assert sleeps.count(16) == 1                                                          # the usual 2/4/8/16 backoff


# ------------------------------------------------------------------ run-level behaviour is unchanged

def test_the_original_requested_window_is_what_the_run_records(pdb):
    r, _, _ = run_sliced({101: boundary_items()})
    run = run_row(pdb, r["run_id"])
    assert (run["window_from_utc"], run["window_to_utc"]) == (SW0, SW1)                  # not a slice
    lo, hi = q(pdb, "SELECT min(request_window_from), max(request_window_to) FROM audit.fetch_attempts "
                    "WHERE run_id=%s AND sensor_id=101", (r["run_id"],))[0]
    assert (lo, hi) == (SW0, SW1)                                                         # the slices tile the window
    assert one(pdb, "SELECT count(DISTINCT request_window_from) FROM audit.fetch_attempts "
                    "WHERE run_id=%s AND sensor_id=101", (r["run_id"],)) == 3             # 30 + 30 + 5 days


def test_a_second_sliced_run_changes_nothing(pdb):
    fleet = {101: boundary_items()}
    first, _, _ = run_sliced(fleet)
    second, _, _ = run_sliced(fleet)
    assert second["status"] == "SUCCESS"
    assert second["bronze_rows_inserted"] == 0
    assert second["bronze_rows_skipped"] == second["bronze_rows_fetched"] == first["bronze_rows_fetched"]
    assert (second["silver_valid"], second["silver_rejected"], second["silver_duplicate"]) == (0, 0, 0)
    assert second["gold_rows_written"] == 0 and second["gold_rows_deleted"] == 0


def test_a_sensor_failing_in_one_slice_counts_once_toward_the_failure_tolerance(pdb):
    fleet = {101: boundary_items(), 102: boundary_items(), 103: failing_slice_fleet()[101]}
    with pytest.raises(PipelineFailed) as e:
        run_sliced(fleet)                                           # default 5 %: 1 of 3 sensors = 33 %
    assert e.value.result["failed_sensor_count"] == 1
    assert "run_fetch_failure_rate" in e.value.result["error_message"]
    assert one(pdb, "SELECT count(*) FROM silver.measurements") == 0                      # Silver/Gold rolled back
    r, _, _ = run_sliced(fleet, max_failed_fraction=0.5)            # within a 50 % tolerance the run succeeds
    assert r["status"] == "SUCCESS" and r["failed_sensor_count"] == 1
    assert one(pdb, "SELECT status FROM audit.reconciliation_checks WHERE run_id=%s AND check_name='run_fetch_failure_rate'",
               (r["run_id"],)) == "WARN"
