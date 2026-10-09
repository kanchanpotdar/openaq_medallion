"""Soft flags: HIGH_VALUE (>500), LONG_GAP (>6h), FLATLINE (>=8 identical AND >=4h).

Every threshold is tested on BOTH sides of its boundary, because off-by-one in a threshold
(> vs >=) is the classic way a data-quality rule silently changes meaning.
"""
from datetime import datetime, timedelta, timezone

from pipeline import run_silver
from tests import helpers

T0 = datetime(2026, 10, 4, 0, 0, tzinfo=timezone.utc)


def _flags(conn, ids):
    return helpers.flags(conn, ids)


def test_high_value_is_strictly_greater_than_500(db):
    ids = helpers.series(db, 1003, T0, 15, [500.0, 500.1, 900, 499.9])
    db.commit()
    run_silver.run(db)
    assert [f["is_high_value"] for f in _flags(db, ids)] == [False, True, True, False]


def test_long_gap_is_strictly_greater_than_6_hours(db):
    s = 1004
    r1 = helpers.reading(db, s, T0, 15, 10)                                         # ends 00:15
    r2 = helpers.reading(db, s, T0 + timedelta(hours=6, minutes=15), 15, 11)        # gap exactly 6h
    r3 = helpers.reading(db, s, T0 + timedelta(hours=12, minutes=31), 15, 12)       # gap 6h01m
    r4 = helpers.reading(db, s, T0 + timedelta(hours=12, minutes=46), 15, 13)       # contiguous
    db.commit()
    run_silver.run(db)
    f = _flags(db, [r1, r2, r3, r4])
    assert [x["is_long_gap"] for x in f] == [False, False, True, False]
    assert [x["gap_before_seconds"] for x in f] == [None, 21600, 21660, 0]


def test_flatline_16_identical_15min_readings_is_exactly_4h_and_flagged(db):
    ids = helpers.series(db, 1005, T0, 15, [25.0] * 16 + [26.0])
    db.commit()
    run_silver.run(db)
    f = _flags(db, ids)
    assert [x["is_flatline"] for x in f] == [True] * 16 + [False]      # 17th value differs
    assert {x["flatline_run_length"] for x in f[:16]} == {16}
    assert f[16]["flatline_run_length"] is None


def test_flatline_not_flagged_when_span_just_under_4h(db):
    ids = helpers.series(db, 1006, T0, 15, [25.0] * 15)                 # 15 x 15min = 3h45m
    db.commit()
    run_silver.run(db)
    assert not any(x["is_flatline"] for x in _flags(db, ids))


def test_flatline_not_flagged_with_7_readings_even_if_span_is_long(db):
    ids = helpers.series(db, 1007, T0, 60, [30.0] * 7)                  # 7 hourly = 7h, but only 7
    db.commit()
    run_silver.run(db)
    assert not any(x["is_flatline"] for x in _flags(db, ids))


def test_flatline_8_hourly_readings_is_flagged(db):
    ids = helpers.series(db, 1008, T0, 60, [30.0] * 8)                  # works for a different interval
    db.commit()
    run_silver.run(db)
    f = _flags(db, ids)
    assert all(x["is_flatline"] for x in f)
    assert {x["flatline_run_length"] for x in f} == {8}


def test_identical_values_across_an_outage_are_not_one_flatline(db):
    """8 + 2h outage + 8 identical readings. Without run-breaking at gaps this would look like
    one 6-hour flatline of 16; with it, each half is only 2 hours."""
    a = helpers.series(db, 1009, T0, 15, [40.0] * 8)
    b = helpers.series(db, 1009, T0 + timedelta(hours=4), 15, [40.0] * 8)
    db.commit()
    run_silver.run(db)
    assert not any(x["is_flatline"] for x in _flags(db, a + b))


def test_soft_flags_annotate_but_never_change_the_verdict(db):
    ids = (helpers.series(db, 1003, T0, 15, [900.0])                    # HIGH_VALUE
           + helpers.series(db, 1005, T0, 15, [25.0] * 16))             # FLATLINE
    db.commit()
    run_silver.run(db)
    v = helpers.verdicts(db)
    assert all(v[i]["classification"] == "VALID" for i in ids)
    assert db.execute("SELECT count(*) FROM silver.rejected_records").fetchone()[0] == 0


def test_late_arriving_reading_retroactively_clears_a_gap_flag(db):
    """Flags depend on neighbours, so they are recomputed when a sensor gets new rows."""
    s = 1010
    helpers.reading(db, s, T0, 15, 1)
    r_late = helpers.reading(db, s, T0 + timedelta(hours=8), 15, 2)
    db.commit()
    run_silver.run(db)
    assert _flags(db, [r_late])[0]["is_long_gap"] is True                # 7h45m gap

    r_mid = helpers.reading(db, s, T0 + timedelta(hours=4), 15, 3)       # arrives in a LATER batch
    db.commit()
    stats = run_silver.run(db)
    assert _flags(db, [r_late])[0]["is_long_gap"] is False               # now only 3h45m
    assert _flags(db, [r_mid])[0]["gap_before_seconds"] == 3 * 3600 + 45 * 60
    assert stats["out_flag_rows_updated"] >= 1
