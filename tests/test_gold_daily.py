"""gold.daily_location_summary: local dates, two-stage averaging, partial days, completeness."""
from datetime import date
from decimal import Decimal

import psycopg
import pytest

from tests import gold_helpers as g
from tests import helpers

pytestmark = pytest.mark.usefixtures("gold_sensors")
ist = g.ist
LOC = lambda sensor_id: sensor_id - 500            # default fixture mapping sensor -> location


def _pair(db):
    helpers.insert_sensor(db, 1020, location_id="900", location_name="Pair", latitude="10.5", longitude="20.5")
    helpers.insert_sensor(db, 1021, location_id="900", location_name="Pair", latitude="11.5", longitude="21.5")
    db.commit()


def test_two_sensor_location_is_the_mean_of_sensor_means_not_pooled(db):
    _pair(db)
    g.default_horizon(db)
    helpers.series(db, 1020, ist(2026, 10, 4, 6), 15, [10, 10, 10, 10, 50, 50, 50, 50])   # hours 06 and 07
    helpers.reading(db, 1021, ist(2026, 10, 4, 6), 60, 30)                                  # hourly sensor, hour 06
    g.build(db)
    (r,) = [x for x in g.daily(db, 900) if x["local_date"] == date(2026, 10, 4)]
    # hour 06: mean(10, 30) = 20 ; hour 07: only sensor 1020 -> 50 ; day = mean(20, 50) = 35.
    # A pooled mean over all 9 readings would be 30, and a per-reading weighting would favour the 15-min sensor.
    assert r["avg_pm25"] == Decimal("35.000")
    assert (r["min_hourly_avg"], r["max_hourly_avg"], r["max_reading"]) == (20, 50, 50)
    assert (r["n_sensors_reporting"], r["n_sensors_at_location"]) == (2, 2)
    assert (r["n_readings"], r["n_location_hours"]) == (9, 2)
    assert r["covered_seconds"] == 7200                  # max across sensors per hour, not the sum
    assert (r["latitude"], r["longitude"]) == (Decimal("10.5"), Decimal("20.5"))   # from the lowest sensor_id


def test_local_date_not_utc_date_decides_the_day(db):
    g.default_horizon(db)
    helpers.reading(db, 1034, ist(2026, 10, 4, 23, 45), 15, 10)      # 18:15Z Oct 4
    helpers.reading(db, 1034, ist(2026, 10, 5, 0, 0), 15, 20)        # 18:30Z Oct 4 : still Oct 4 in UTC
    g.build(db)
    rows = g.daily(db, LOC(1034))
    assert [(r["local_date"], r["n_readings"], r["avg_pm25"]) for r in rows] == [
        (date(2026, 10, 4), 1, 10), (date(2026, 10, 5), 1, 20)]


def test_edge_days_are_partial_and_expected_time_comes_from_the_horizon(db):
    g.default_horizon(db)                                # horizon: IST Oct 3 06:00 -> Oct 5 12:00
    g.build(db)
    first, last = g.daily(db, LOC(g.HORIZON_SENSOR))
    assert (first["local_date"], first["expected_seconds"], first["is_partial_day"]) == (date(2026, 10, 3), 64800, True)
    assert (last["local_date"], last["expected_seconds"], last["is_partial_day"]) == (date(2026, 10, 5), 43200, True)
    assert first["covered_seconds"] == 900 and first["coverage_pct"] == Decimal("1.39")
    # the silent middle day (Oct 4) has NO daily row for this location
    assert len(g.daily(db, LOC(g.HORIZON_SENSOR))) == 2


def test_full_day_with_every_hour_is_100_percent_and_not_partial(db):
    g.default_horizon(db)
    helpers.series(db, 1030, ist(2026, 10, 4, 0), 60, g.varied(24))
    g.build(db)
    (r,) = [x for x in g.daily(db, LOC(1030)) if x["local_date"] == date(2026, 10, 4)]
    assert (r["expected_seconds"], r["covered_seconds"], r["coverage_pct"]) == (86400, 86400, 100)
    assert (r["is_partial_day"], r["meets_completeness"], r["n_location_hours"]) == (False, True, 24)


def test_completeness_boundary_is_75_percent_and_inclusive(db):
    g.default_horizon(db)
    helpers.series(db, 1031, ist(2026, 10, 4, 0), 60, g.varied(18))    # 18 of 24 hours = exactly 75%
    helpers.series(db, 1032, ist(2026, 10, 4, 0), 60, g.varied(17))    # 17 of 24 hours = 70.83%
    g.build(db)
    at = g.daily(db, LOC(1031))[0]
    below = g.daily(db, LOC(1032))[0]
    assert (at["coverage_pct"], at["meets_completeness"]) == (75, True)
    assert (below["coverage_pct"], below["meets_completeness"]) == (Decimal("70.83"), False)


def test_completeness_threshold_is_a_function_parameter(db):
    g.default_horizon(db)
    helpers.series(db, 1032, ist(2026, 10, 4, 0), 60, g.varied(17))    # 70.83%
    g.build(db)
    assert g.daily(db, LOC(1032))[0]["meets_completeness"] is False
    stats = g.build(db, p_completeness_pct=70)
    assert g.daily(db, LOC(1032))[0]["meets_completeness"] is True
    assert stats["out_daily_updated"] >= 1               # the flag flipped through the diff-upsert path


def test_flatline_and_high_value_roll_up_and_stay_visible(db):
    g.default_horizon(db)
    helpers.series(db, 1033, ist(2026, 10, 4, 0), 15, [600.0] * 16)    # 4h flat run, all > 500
    g.build(db)
    (r,) = [x for x in g.daily(db, LOC(1033)) if x["local_date"] == date(2026, 10, 4)]
    assert r["avg_pm25"] == 600
    assert r["avg_pm25_excl_flatline"] is None            # every reading is flagged
    assert (r["n_flatline_readings"], r["n_high_value_readings"], r["max_reading"]) == (16, 16, 600)


def test_a_location_whose_sensors_disagree_on_timezone_aborts_the_build(db):
    helpers.insert_sensor(db, 1040, location_id="950")
    helpers.insert_sensor(db, 1041, location_id="950", timezone="UTC")
    db.commit()
    with pytest.raises(psycopg.errors.RaiseException, match="different timezones"):
        g.build(db)
    db.rollback()
    assert g.gold_counts(db) == dict(hourly=0, daily=0, health=0)
