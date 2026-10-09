"""gold.sensor_health: derived expectations (never 96), silent sensors, gaps, status boundaries."""
import pytest
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from tests import gold_helpers as g
from tests import helpers

pytestmark = pytest.mark.usefixtures("gold_sensors")
ist = g.ist
UTC = timezone.utc
OCT4 = (2026, 10, 4)


def test_expected_readings_follow_each_sensors_own_interval(db):
    g.default_horizon(db)
    helpers.series(db, 1050, ist(2026, 10, 4, 0), 5, g.varied(288))      # 5-minute
    helpers.series(db, 1051, ist(2026, 10, 4, 0), 15, g.varied(96))      # 15-minute
    helpers.series(db, 1052, ist(2026, 10, 4, 0), 60, g.varied(24))      # hourly
    g.build(db)
    got = {sid: g.day_rows(db, sid, *OCT4) for sid in (1050, 1051, 1052)}
    assert [got[s]["expected_interval_seconds"] for s in (1050, 1051, 1052)] == [300, 900, 3600]
    assert [got[s]["expected_readings"] for s in (1050, 1051, 1052)] == [288, 96, 24]
    assert [got[s]["received_readings"] for s in (1050, 1051, 1052)] == [288, 96, 24]
    assert all(r["coverage_pct"] == 100 and r["health_status"] == "OK" for r in got.values())


def test_never_reported_sensor_is_listed_as_no_data_with_null_interval(db):
    g.default_horizon(db)
    g.build(db)
    rows = g.health(db, 1005)                              # fixture sensor 1005 never reports
    assert [r["local_date"].day for r in rows] == [3, 4, 5]
    assert all(r["expected_interval_seconds"] is None and r["expected_readings"] is None for r in rows)
    assert all(r["received_readings"] == 0 and r["coverage_pct"] == 0 for r in rows)
    assert all(r["health_status"] == "NO_DATA" for r in rows)
    # with nothing received the whole effective window is one gap: 18 h, 24 h, 12 h
    assert [r["longest_gap_minutes"] for r in rows] == [1080, 1440, 720]


def test_every_sensor_gets_a_row_for_every_local_day_in_the_horizon(db):
    g.default_horizon(db)
    g.build(db)
    n_sensors = db.execute("SELECT count(*) FROM silver.sensors").fetchone()[0]
    assert db.execute("SELECT count(*) FROM gold.sensor_health").fetchone()[0] == n_sensors * 3


def test_a_silent_day_keeps_the_sensors_historical_interval(db):
    g.default_horizon(db)
    helpers.series(db, 1061, ist(2026, 10, 3, 6), 60, g.varied(18))     # reports Oct 3 only
    g.build(db)
    oct4, oct5 = g.day_rows(db, 1061, *OCT4), g.day_rows(db, 1061, 2026, 10, 5)
    assert (oct4["health_status"], oct4["expected_interval_seconds"], oct4["expected_readings"]) == ("NO_DATA", 3600, 24)
    assert (oct5["expected_readings"], oct5["expected_seconds"]) == (12, 43200)


def test_edge_days_expect_fewer_readings_because_the_horizon_cuts_them_short(db):
    g.default_horizon(db)
    helpers.series(db, 1051, ist(2026, 10, 4, 0), 15, g.varied(96))
    g.build(db)
    d3, d4, d5 = (g.day_rows(db, 1051, 2026, 10, x) for x in (3, 4, 5))
    assert [(d["expected_seconds"], d["is_partial_day"]) for d in (d3, d4, d5)] == [
        (64800, True), (86400, False), (43200, True)]
    assert [d["expected_readings"] for d in (d3, d4, d5)] == [72, 96, 48]


def test_longest_gap_covers_head_internal_and_tail_gaps(db):
    g.default_horizon(db)
    s = 1062
    helpers.series(db, s, ist(2026, 10, 4, 0), 15, g.varied(4))           # 00:00-01:00
    helpers.series(db, s, ist(2026, 10, 4, 5), 15, g.varied(4, 10))       # 05:00-06:00
    helpers.series(db, s, ist(2026, 10, 4, 20), 15, g.varied(16, 20))     # 20:00-24:00
    g.build(db)
    d3, d4, d5 = (g.day_rows(db, s, 2026, 10, x) for x in (3, 4, 5))
    assert d3["longest_gap_minutes"] == 1080 and d3["health_status"] == "NO_DATA"   # head: horizon start -> first reading
    assert d4["longest_gap_minutes"] == 840                                          # 06:00 -> 20:00 beats 01:00 -> 05:00
    assert d4["received_readings"] == 24 and d4["coverage_pct"] == 25 and d4["health_status"] == "LOW_COVERAGE"
    assert d5["longest_gap_minutes"] == 720 and d5["health_status"] == "NO_DATA"     # tail: last reading -> horizon end


def test_utc_aligned_hourly_sensor_has_no_artificial_gap_on_a_full_day(db):
    g.default_horizon(db)
    start = datetime(2026, 10, 3, 12, 0, tzinfo=UTC)
    helpers.series(db, 1063, start, 60, g.varied(48))                    # hourly readings on UTC hours
    g.build(db)
    r = g.day_rows(db, 1063, *OCT4)
    assert r["received_readings"] == 24 and r["coverage_pct"] == 100
    assert r["longest_gap_minutes"] == 0                                 # NOT 30: whole-series gaps, clipped to the day
    assert r["health_status"] == "OK"


def test_status_boundaries_use_strict_less_than_on_coverage(db):
    g.default_horizon(db)
    for sid, n in ((1070, 22), (1071, 21), (1072, 12), (1073, 11)):      # of 24 expected hourly readings
        helpers.series(db, sid, ist(2026, 10, 4, 0), 60, g.varied(n))
    g.build(db)
    st = lambda sid: g.day_rows(db, sid, *OCT4)["health_status"]
    cov = lambda sid: g.day_rows(db, sid, *OCT4)["coverage_pct"]
    assert (cov(1070), st(1070)) == (Decimal("91.67"), "OK")
    assert (cov(1071), st(1071)) == (Decimal("87.50"), "PARTIAL")        # < 90
    assert (cov(1072), st(1072)) == (Decimal("50.00"), "PARTIAL")        # exactly 50 is NOT low coverage
    assert (cov(1073), st(1073)) == (Decimal("45.83"), "LOW_COVERAGE")   # < 50
    # thresholds are function parameters, applied with the same strict comparison
    g.build(db, p_partial_coverage_pct=87.5)
    assert st(1071) == "OK"                                              # 87.5 is not < 87.5
    g.build(db, p_partial_coverage_pct=87.51)
    assert st(1071) == "PARTIAL"
    g.build(db, p_low_coverage_pct=50.01)
    assert st(1072) == "LOW_COVERAGE"


def test_stuck_threshold_is_inclusive_and_beats_low_coverage(db):
    g.default_horizon(db)
    helpers.series(db, 1074, ist(2026, 10, 4, 0), 60, [30] * 9 + [31])           # 9 of 10 flagged = 90.00%
    helpers.series(db, 1075, ist(2026, 10, 4, 0), 60, [30] * 8 + [31, 32])       # 8 of 10 flagged = 80.00%
    g.build(db)
    a, b = g.day_rows(db, 1074, *OCT4), g.day_rows(db, 1075, *OCT4)
    assert (a["flatline_pct"], a["coverage_pct"], a["health_status"]) == (90, Decimal("41.67"), "STUCK")
    assert (b["flatline_pct"], b["health_status"]) == (80, "LOW_COVERAGE")
    g.build(db, p_stuck_flatline_pct=90.01)
    assert g.day_rows(db, 1074, *OCT4)["health_status"] == "LOW_COVERAGE"


def test_a_stuck_sensor_can_have_perfect_coverage(db):
    g.default_horizon(db)
    helpers.series(db, 1076, ist(2026, 10, 4, 0), 15, [12.34] * 96)
    g.build(db)
    r = g.day_rows(db, 1076, *OCT4)
    assert (r["coverage_pct"], r["flatline_pct"], r["n_flatline_readings"], r["health_status"]) == (100, 100, 96, "STUCK")


def test_flag_counts_are_carried_into_health(db):
    g.default_horizon(db)
    helpers.series(db, 1077, ist(2026, 10, 4, 0), 60, [601, 602, 603, 604, 605])
    g.build(db)
    r = g.day_rows(db, 1077, *OCT4)
    assert r["n_high_value_readings"] == 5 and r["n_flatline_readings"] == 0


def test_coverage_is_time_based_when_a_sensor_changes_interval(db):
    g.default_horizon(db)
    helpers.series(db, 1078, ist(2026, 10, 4, 0), 15, g.varied(48))              # 00:00-12:00 every 15 min
    helpers.series(db, 1078, ist(2026, 10, 4, 12), 60, g.varied(12, 100))        # 12:00-24:00 hourly
    g.build(db)
    r = g.day_rows(db, 1078, *OCT4)
    assert (r["received_readings"], r["expected_interval_seconds"], r["expected_readings"]) == (60, 900, 96)
    assert (r["covered_seconds"], r["coverage_pct"]) == (86400, 100)             # reading COUNT would say 62.5%
    assert r["longest_gap_minutes"] == 0
