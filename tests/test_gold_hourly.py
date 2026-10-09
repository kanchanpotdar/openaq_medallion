"""gold.hourly_sensor_pm25: local-hour bucketing, duration weighting, coverage, flags."""
import pytest
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from tests import gold_helpers as g
from tests import helpers

UTC = timezone.utc
pytestmark = pytest.mark.usefixtures("gold_sensors")
ist = g.ist


def test_four_15min_readings_make_a_full_local_hour(db):
    helpers.series(db, 1001, ist(2026, 10, 3, 6), 15, [10, 20, 30, 40])
    g.build(db)
    (r,) = g.hourly(db, 1001)
    assert r["avg_pm25"] == Decimal("25.000")
    assert (r["min_pm25"], r["max_pm25"], r["n_readings"]) == (10, 40, 4)
    assert (r["reading_interval_seconds"], r["covered_seconds"], r["coverage_pct"]) == (900, 3600, 100)
    assert r["local_hour_start"] == datetime(2026, 10, 3, 6, 0)
    assert r["local_date"] == date(2026, 10, 3)
    assert r["hour_start_utc"] == datetime(2026, 10, 3, 0, 30, tzinfo=UTC)   # IST hour start == :30 UTC


def test_missing_readings_lower_coverage_not_the_average(db):
    helpers.series(db, 1001, ist(2026, 10, 3, 6), 15, [10, 20, 30])             # 3 of 4 present
    g.build(db)
    (r,) = g.hourly(db, 1001)
    assert (r["n_readings"], r["covered_seconds"], r["coverage_pct"]) == (3, 2700, 75)
    assert r["avg_pm25"] == 20


def test_average_is_duration_weighted_not_a_plain_mean(db):
    helpers.reading(db, 1001, ist(2026, 10, 3, 7, 0), 15, 10)      # 900 s of 10
    helpers.reading(db, 1001, ist(2026, 10, 3, 7, 15), 30, 40)     # 1800 s of 40
    g.build(db)
    (r,) = g.hourly(db, 1001)
    assert r["avg_pm25"] == Decimal("30.000")          # (10*900 + 40*1800) / 2700 ; plain mean would be 25
    assert r["covered_seconds"] == 2700


def test_hour_bucket_is_the_local_hour_not_the_utc_hour(db):
    helpers.reading(db, 1001, datetime(2026, 10, 3, 0, 0, tzinfo=UTC), 15, 10)   # 05:30 IST
    g.build(db)
    (r,) = g.hourly(db, 1001)
    assert r["local_hour_start"] == datetime(2026, 10, 3, 5, 0)
    assert r["hour_start_utc"] == datetime(2026, 10, 2, 23, 30, tzinfo=UTC)


def test_utc_aligned_hourly_readings_are_attributed_to_their_start_hour(db):
    """Documented 30-minute smear: a 00:00-01:00Z reading spans 05:30-06:30 IST but counts
    wholly toward the 05:00 local hour. Each reading lands in exactly one hour."""
    helpers.reading(db, 1001, datetime(2026, 10, 3, 0, 0, tzinfo=UTC), 60, 64.1)
    helpers.reading(db, 1001, datetime(2026, 10, 3, 1, 0, tzinfo=UTC), 60, 59.2)
    g.build(db)
    r = g.hourly(db, 1001)
    assert [x["local_hour_start"] for x in r] == [datetime(2026, 10, 3, 5), datetime(2026, 10, 3, 6)]
    assert [x["n_readings"] for x in r] == [1, 1]
    assert [x["coverage_pct"] for x in r] == [100, 100]


def test_local_midnight_boundary_decides_the_local_date(db):
    helpers.reading(db, 1001, ist(2026, 10, 2, 23, 45), 15, 10)
    helpers.reading(db, 1001, ist(2026, 10, 3, 0, 0), 15, 20)
    g.build(db)
    a, b = g.hourly(db, 1001)
    assert (a["local_date"], b["local_date"]) == (date(2026, 10, 2), date(2026, 10, 3))
    assert b["hour_start_utc"] == datetime(2026, 10, 2, 18, 30, tzinfo=UTC)


def test_flatline_is_kept_in_the_average_and_also_excluded_in_the_second_column(db):
    # 16 identical 15-min readings from 06:30 (=4h exactly) are flagged FLATLINE by Silver,
    # then two normal readings.
    helpers.series(db, 1001, ist(2026, 10, 3, 6, 30), 15, [25.0] * 16)
    helpers.reading(db, 1001, ist(2026, 10, 3, 10, 30), 15, 40)
    helpers.reading(db, 1001, ist(2026, 10, 3, 10, 45), 15, 50)
    g.build(db)
    by_hour = {r["local_hour_start"].hour: r for r in g.hourly(db, 1001)}
    first = by_hour[6]                                   # 06:30 and 06:45: both flagged
    assert (first["avg_pm25"], first["avg_pm25_excl_flatline"], first["n_flatline_readings"]) == (25, None, 2)
    mixed = by_hour[10]                                  # 25,25 flagged ; 40,50 normal
    assert mixed["avg_pm25"] == 35                       # primary average KEEPS flagged readings
    assert mixed["avg_pm25_excl_flatline"] == 45
    assert (mixed["n_readings"], mixed["n_flatline_readings"]) == (4, 2)


def test_high_value_is_averaged_counted_and_never_hidden(db):
    helpers.series(db, 1001, ist(2026, 10, 3, 6), 15, [500.0, 600.0, 700.0, 100.0])
    g.build(db)
    (r,) = g.hourly(db, 1001)
    assert r["avg_pm25"] == 475                          # all four readings are in the mean
    assert r["max_pm25"] == 700
    assert r["n_high_value_readings"] == 2               # 600 and 700; 500 is not > 500


def test_zero_is_a_real_average_not_null(db):
    helpers.series(db, 1001, ist(2026, 10, 3, 6), 15, [0, 0, 0, 0])
    g.build(db)
    (r,) = g.hourly(db, 1001)
    assert r["avg_pm25"] == 0 and r["min_pm25"] == 0


def test_overlapping_readings_cannot_push_coverage_above_100(db):
    helpers.reading(db, 1001, ist(2026, 10, 3, 6, 0), 60, 10)
    helpers.reading(db, 1001, ist(2026, 10, 3, 6, 5), 15, 20)       # overlaps the first
    g.build(db)
    (r,) = g.hourly(db, 1001)
    assert (r["covered_seconds"], r["coverage_pct"], r["n_readings"]) == (3600, 100, 2)
    assert r["avg_pm25"] == 12                                       # (10*3600 + 20*900) / 4500


def test_dst_local_hours_use_the_real_utc_offset(db):
    helpers.insert_sensor(db, 1101, timezone="America/New_York")
    db.commit()
    helpers.reading(db, 1101, datetime(2026, 3, 8, 6, 45, tzinfo=UTC), 15, 10)   # 01:45 EST
    helpers.reading(db, 1101, datetime(2026, 3, 8, 7, 0, tzinfo=UTC), 15, 20)    # 03:00 EDT (clocks sprang forward)
    g.build(db)
    a, b = g.hourly(db, 1101)
    assert (a["local_hour_start"], a["hour_start_utc"]) == (datetime(2026, 3, 8, 1), datetime(2026, 3, 8, 6, tzinfo=UTC))
    assert (b["local_hour_start"], b["hour_start_utc"]) == (datetime(2026, 3, 8, 3), datetime(2026, 3, 8, 7, tzinfo=UTC))


def test_only_valid_readings_feed_gold_and_silent_sensors_get_no_hourly_rows(db):
    ids = helpers.load_cases(db)                         # dirty fixture: rejects, duplicates, valid rows
    g.build(db)
    expected_valid = sum(c["expected_class"] == "VALID" for c in helpers.read_cases())
    total = db.execute("SELECT sum(n_readings) FROM gold.hourly_sensor_pm25").fetchone()[0]
    assert total == expected_valid
    sensors = {r[0] for r in db.execute("SELECT DISTINCT sensor_id FROM gold.hourly_sensor_pm25").fetchall()}
    assert sensors == {1001, 1002}                       # the other 11 fixture sensors never reported
