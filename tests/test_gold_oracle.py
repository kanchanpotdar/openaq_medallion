"""Independent Python oracle vs gold.run_gold() on a randomised dataset.

The generator mixes 5/15/30/60-minute sensors, UTC-aligned and local-aligned hourly readings, an interval
change inside one sensor, dropouts, flat stretches, spikes, a two-sensor location, a silent sensor, and a
New York sensor whose data crosses the 2026-03-08 daylight-saving change. EVERY Gold row is compared."""
import random
import pytest
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from tests import gold_helpers as g
from tests import helpers
from tests import oracle

UTC = timezone.utc
pytestmark = pytest.mark.usefixtures("gold_sensors")


def _fill(db, rnd, sensor, start, end, step, keep, *, flat=None, spikes=0, tag="r"):
    """Readings every `step` minutes in [start, end). flat=(first_slot, n_slots, value) forces a flat run."""
    slots = int((end - start).total_seconds() // (step * 60))
    spike_slots = set(rnd.sample(range(slots), spikes)) if spikes else set()
    for i in range(slots):
        in_flat = flat and flat[0] <= i < flat[0] + flat[1]
        if not in_flat and rnd.random() > keep:
            continue
        value = flat[2] if in_flat else (600.0 if i in spike_slots else round(rnd.uniform(5, 150), 2))
        helpers.reading(db, sensor, start + timedelta(minutes=step * i), step, value, tag)


def _build_dataset(db):
    rnd = random.Random(20261009)
    helpers.insert_sensor(db, 1101, location_id="800", location_name="Pair", latitude="12.5", longitude="77.5")
    helpers.insert_sensor(db, 1102, location_id="800", location_name="Pair", latitude="12.6", longitude="77.6")
    helpers.insert_sensor(db, 1104, location_id="801", timezone="America/New_York")
    db.commit()
    d = lambda day, h=0: datetime(2026, 3, day, h, tzinfo=UTC)
    _fill(db, rnd, 1001, d(6, 12), d(9, 12), 15, 0.85, flat=(100, 40, 33.0), spikes=3)   # 15-min, 10 h flatline
    _fill(db, rnd, 1002, d(7, 3), d(8, 3), 5, 0.5)                                       # 5-min, sparse
    _fill(db, rnd, 1003, d(6, 0), d(10, 0), 60, 0.9, flat=(20, 16, 12.0))                # hourly on UTC hours
    _fill(db, rnd, 1004, d(7, 0), d(7, 12), 60, 0.95, tag="a")                           # interval change:
    _fill(db, rnd, 1004, d(7, 12), d(8, 12), 30, 0.9, tag="b")                           #   60 min -> 30 min
    _fill(db, rnd, 1101, d(7, 6), d(9, 6), 15, 0.7)                                      # two sensors, one location
    _fill(db, rnd, 1102, d(7, 6), d(9, 6), 60, 0.9)
    _fill(db, rnd, 1104, d(7, 20), d(9, 12), 15, 0.9)                                    # New York, crosses DST start
    # sensors 1005..1012 never report: they must still appear in sensor_health


def test_oracle_agrees_with_gold_on_every_row_with_default_thresholds(db):
    _build_dataset(db)
    g.build(db)
    mismatches, n = oracle.compare(db)
    assert n["hourly"] > 300 and n["daily"] > 10 and n["health"] > 60          # the dataset is not trivially small
    assert mismatches == [], f"{len(mismatches)} mismatches, first: {mismatches[:5]}"


def test_oracle_agrees_when_every_threshold_is_overridden(db):
    _build_dataset(db)
    g.build(db, p_completeness_pct=60, p_stuck_flatline_pct=80, p_low_coverage_pct=40, p_partial_coverage_pct=85)
    mismatches, _ = oracle.compare(db, completeness=60, stuck=80, low=40, partial=85)
    assert mismatches == [], f"{len(mismatches)} mismatches, first: {mismatches[:5]}"


def test_dataset_exercises_the_interesting_cases(db):
    """Guard the guard: the randomised data must actually contain what the oracle is meant to check."""
    _build_dataset(db)
    g.build(db)
    q = lambda sql: db.execute(sql).fetchone()[0]
    assert q("SELECT count(*) FROM silver.measurements WHERE is_flatline") > 0
    assert q("SELECT count(*) FROM silver.measurements WHERE is_high_value") > 0
    assert q("SELECT count(DISTINCT duration_seconds) FROM silver.measurements") >= 4
    assert q("SELECT count(*) FROM gold.daily_location_summary WHERE n_sensors_reporting = 2") > 0
    assert q("SELECT count(*) FROM gold.sensor_health WHERE health_status = 'NO_DATA'") > 0
    assert q("SELECT count(*) FROM gold.sensor_health WHERE is_partial_day") > 0
    assert q("SELECT count(DISTINCT health_status) FROM gold.sensor_health") >= 3
    assert q("SELECT count(*) FROM gold.hourly_sensor_pm25 WHERE timezone = 'America/New_York'") > 0
