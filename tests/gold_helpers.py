"""Helpers for the Gold tests (built on tests/helpers.py)."""
from datetime import datetime, timedelta, timezone

from pipeline import run_gold, run_silver
from tests import helpers

UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))
HORIZON_SENSOR = 1013          # carries two tiny readings that define the data horizon


def ist(y, m, d, h=0, mi=0):
    return datetime(y, m, d, h, mi, tzinfo=IST)


def build(conn, **thresholds):
    """Silver (incremental) then Gold, committed. Returns the run_gold stats."""
    run_silver.run(conn)
    stats = run_gold.run(conn, **thresholds)
    conn.commit()
    return stats


def rows(conn, sql, params=()):
    cur = conn.execute(sql, params)
    cols = [d.name for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def set_horizon(conn, start, end):
    """Two 15-minute readings on a dedicated sensor so the data horizon is exactly [start, end]."""
    helpers.reading(conn, HORIZON_SENSOR, start, 15, 1)
    helpers.reading(conn, HORIZON_SENSOR, end - timedelta(minutes=15), 15, 2)


def default_horizon(conn):
    """IST 2026-10-03 06:00 -> 2026-10-05 12:00: Oct 4 is a full local day, Oct 3 and 5 are partial."""
    set_horizon(conn, ist(2026, 10, 3, 6), ist(2026, 10, 5, 12))


def hourly(conn, sensor_id):
    return rows(conn, "SELECT * FROM gold.hourly_sensor_pm25 WHERE sensor_id=%s ORDER BY hour_start_utc", (sensor_id,))


def daily(conn, location_id):
    return rows(conn, "SELECT * FROM gold.daily_location_summary WHERE location_id=%s ORDER BY local_date", (location_id,))


def health(conn, sensor_id):
    return rows(conn, "SELECT * FROM gold.sensor_health WHERE sensor_id=%s ORDER BY local_date", (sensor_id,))


def day_rows(conn, sensor_id, y, m, d):
    from datetime import date
    r = rows(conn, "SELECT * FROM gold.sensor_health WHERE sensor_id=%s AND local_date=%s", (sensor_id, date(y, m, d)))
    return r[0]


def varied(n, base=1, mod=40):
    """n values where consecutive readings always differ (so no accidental flatline)."""
    return [base + (i % mod) for i in range(n)]


def snapshot(conn):
    """Deterministic content of all three Gold tables (no run ids / timestamps)."""
    out = []
    for table, order in (("gold.hourly_sensor_pm25", "sensor_id, hour_start_utc"),
                         ("gold.daily_location_summary", "location_id, local_date"),
                         ("gold.sensor_health", "sensor_id, local_date")):
        out.append(conn.execute(
            f"SELECT to_jsonb(t) - 'gold_run_id' - 'built_at' FROM {table} t ORDER BY {order}").fetchall())
    return tuple(out)


def gold_counts(conn):
    q = lambda t: conn.execute(f"SELECT count(*) FROM gold.{t}").fetchone()[0]
    return dict(hourly=q("hourly_sensor_pm25"), daily=q("daily_location_summary"), health=q("sensor_health"))
