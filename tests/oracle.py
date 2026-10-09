"""Independent re-implementation of the Gold formulas in plain Python (Decimal + zoneinfo).

It reads ONLY silver.measurements / silver.sensors and recomputes hourly, daily and health
rows with no SQL aggregation, then compares them with what gold.run_gold() wrote. Because it
shares no code with the SQL, a misconception baked into the SQL cannot also hide in the test.

Run against the real database:   python -m tests.oracle --dbname openaq
"""
import argparse
import sys
from collections import Counter, defaultdict
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from zoneinfo import ZoneInfo

import psycopg

from pipeline import config

UTC = timezone.utc
D = Decimal


def rnd(x, places):
    return D(x).quantize(D(1).scaleb(-places), rounding=ROUND_HALF_UP)


def mode_smallest(values):
    c = Counter(values)
    top = max(c.values())
    return min(v for v, n in c.items() if n == top)     # ties -> smallest, like mode() WITHIN GROUP


def mean(xs):
    return sum(xs, D(0)) / D(len(xs))


# ------------------------------------------------------------------ load

def load(conn):
    sensors = {}
    for sid, loc, tz, name, lat, lon in conn.execute(
            "SELECT sensor_id, location_id, timezone, location_name, latitude, longitude FROM silver.sensors"):
        sensors[sid] = dict(loc=loc, tz=ZoneInfo(tz), name=name, lat=lat, lon=lon)
    readings = []
    for sid, f, t, dur, v, flat, high, lg in conn.execute(
            "SELECT sensor_id, period_from_utc, period_to_utc, duration_seconds, pm25_ugm3, "
            "is_flatline, is_high_value, is_long_gap FROM silver.measurements"):
        readings.append(dict(sid=sid, f=f.astimezone(UTC), t=t.astimezone(UTC), dur=dur,
                             v=D(v), flat=flat, high=high, lg=lg))
    return sensors, readings


# ------------------------------------------------------------------ hourly

def hourly(sensors, readings):
    groups = defaultdict(list)
    for r in readings:
        lf = r["f"].astimezone(sensors[r["sid"]]["tz"])
        hs = lf.replace(minute=0, second=0, microsecond=0)     # LOCAL hour of the reading's START
        groups[(r["sid"], hs.astimezone(UTC))].append((r, hs))
    out = {}
    for (sid, hu), items in groups.items():
        rs = [r for r, _ in items]
        hs = items[0][1]
        tot = sum(r["dur"] for r in rs)
        keep = [r for r in rs if not r["flat"]]
        covered = min(3600, tot)
        out[(sid, hu)] = dict(
            sid=sid, loc=sensors[sid]["loc"], local_hour=hs.replace(tzinfo=None), local_date=hs.date(),
            avg=rnd(sum(r["v"] * r["dur"] for r in rs) / tot, 3),
            excl=(rnd(sum(r["v"] * r["dur"] for r in keep) / sum(r["dur"] for r in keep), 3) if keep else None),
            mn=min(r["v"] for r in rs), mx=max(r["v"] for r in rs), n=len(rs),
            interval=mode_smallest([r["dur"] for r in rs]), covered=covered,
            coverage=rnd(D(100) * covered / 3600, 2),
            nflat=sum(r["flat"] for r in rs), nhigh=sum(r["high"] for r in rs))
    return out


# ------------------------------------------------------------------ daily

def day_bounds(d, tz):
    start = datetime.combine(d, time.min, tzinfo=tz).astimezone(UTC)
    end = datetime.combine(d + timedelta(days=1), time.min, tzinfo=tz).astimezone(UTC)
    return start, end


def daily(hourly_exp, sensors, h0, h1, completeness):
    by_lh = defaultdict(list)
    for h in hourly_exp.values():
        by_lh[(h["loc"], h["local_hour"])].append(h)
    by_day = defaultdict(list)
    for (loc, lh), hs in by_lh.items():
        excl = [h["excl"] for h in hs if h["excl"] is not None]
        by_day[(loc, lh.date())].append(dict(
            lavg=mean([h["avg"] for h in hs]), lexcl=(mean(excl) if excl else None),
            covered=max(h["covered"] for h in hs), n=sum(h["n"] for h in hs),
            mx=max(h["mx"] for h in hs), nflat=sum(h["nflat"] for h in hs),
            nhigh=sum(h["nhigh"] for h in hs), sids={h["sid"] for h in hs}))
    loc_sensors = defaultdict(list)
    for sid, s in sensors.items():
        loc_sensors[s["loc"]].append(sid)
    out = {}
    for (loc, d), hours in by_day.items():
        first = sensors[min(loc_sensors[loc])]
        tz = first["tz"]
        ds, de = day_bounds(d, tz)
        expected = int((min(de, h1) - max(ds, h0)).total_seconds())
        covered = min(sum(h["covered"] for h in hours), expected)
        coverage = rnd(D(100) * covered / expected, 2)
        excl = [h["lexcl"] for h in hours if h["lexcl"] is not None]
        out[(loc, d)] = dict(
            name=first["name"], lat=first["lat"], lon=first["lon"],
            avg=rnd(mean([h["lavg"] for h in hours]), 3),
            excl=(rnd(mean(excl), 3) if excl else None),
            min_h=rnd(min(h["lavg"] for h in hours), 3), max_h=rnd(max(h["lavg"] for h in hours), 3),
            max_reading=max(h["mx"] for h in hours),
            n_reporting=len(set().union(*[h["sids"] for h in hours])),
            n_at_location=len(loc_sensors[loc]), n=sum(h["n"] for h in hours), n_hours=len(hours),
            expected=expected, covered=covered, coverage=coverage,
            partial=(expected < int((de - ds).total_seconds())),
            complete=(coverage >= D(completeness)),
            nflat=sum(h["nflat"] for h in hours), nhigh=sum(h["nhigh"] for h in hours))
    return out


# ------------------------------------------------------------------ health

def health(sensors, readings, h0, h1, p):
    by_sensor = defaultdict(list)
    for r in readings:
        by_sensor[r["sid"]].append(r)
    out = {}
    for sid, s in sensors.items():
        tz = s["tz"]
        rs = sorted(by_sensor.get(sid, []), key=lambda r: r["f"])
        all_mode = mode_smallest([r["dur"] for r in rs]) if rs else None
        gaps, prev_end = [], None                         # gaps on the WHOLE series, clipped per day below
        for r in rs:
            start = prev_end if prev_end is not None else h0
            if r["f"] > start:
                gaps.append((start, r["f"]))
            prev_end = r["t"] if prev_end is None else max(prev_end, r["t"])
        if not rs:
            gaps.append((h0, h1))
        elif prev_end < h1:
            gaps.append((prev_end, h1))
        per_day = defaultdict(list)
        for r in rs:
            per_day[r["f"].astimezone(tz).date()].append(r)
        d = h0.astimezone(tz).date()
        last = (h1 - timedelta(microseconds=1)).astimezone(tz).date()
        while d <= last:
            ds, de = day_bounds(d, tz)
            es, ee = max(ds, h0), min(de, h1)
            eff = int((ee - es).total_seconds())
            dr = per_day.get(d, [])
            n = len(dr)
            covered = min(sum(r["dur"] for r in dr), eff)
            interval = mode_smallest([r["dur"] for r in dr]) if dr else all_mode
            longest = max([(min(ge, ee) - max(gs, es)).total_seconds()
                           for gs, ge in gaps if gs < ee and ge > es] or [0])
            coverage = rnd(D(100) * covered / eff, 2)
            nflat = sum(r["flat"] for r in dr)
            flat_pct = rnd(D(100) * nflat / n, 2) if n else None
            if n == 0:
                status = "NO_DATA"
            elif flat_pct >= D(p["stuck"]):
                status = "STUCK"
            elif coverage < D(p["low"]):
                status = "LOW_COVERAGE"
            elif coverage < D(p["partial"]):
                status = "PARTIAL"
            else:
                status = "OK"
            out[(sid, d)] = dict(
                loc=s["loc"], interval=interval, expected_seconds=eff,
                expected_readings=(int(rnd(D(eff) / interval, 0)) if interval else None),
                received=n, covered=covered, coverage=coverage,
                longest_gap=rnd(D(longest) / 60, 1), nflat=nflat,
                nhigh=sum(r["high"] for r in dr), nlg=sum(r["lg"] for r in dr), flat_pct=flat_pct,
                first=min((r["f"] for r in dr), default=None), last=max((r["f"] for r in dr), default=None),
                partial=(ee - es) < (de - ds), status=status)
            d += timedelta(days=1)
    return out


# ------------------------------------------------------------------ compare

def _eq(a, b, tol=None):
    if a is None or b is None:
        return a is None and b is None
    if tol is not None:
        return abs(D(a) - D(b)) <= D(str(tol))
    return a == b


def _rows(conn, sql):
    cur = conn.execute(sql)
    cols = [c.name for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def compare(conn, completeness=75, stuck=90, low=50, partial=90):
    """Return (mismatches, counts). mismatches is a list of human-readable strings."""
    sensors, readings = load(conn)
    if not readings:
        return [], dict(hourly=0, daily=0, health=0)
    h0 = min(r["f"] for r in readings)
    h1 = max(r["t"] for r in readings)
    eh = hourly(sensors, readings)
    ed = daily(eh, sensors, h0, h1, completeness)
    ee = health(sensors, readings, h0, h1, dict(stuck=stuck, low=low, partial=partial))
    bad = []

    def check(label, gold_rows, expected, keyf, fields):
        gk = {keyf(r): r for r in gold_rows}
        if set(gk) != set(expected):
            only_g, only_e = set(gk) - set(expected), set(expected) - set(gk)
            bad.append(f"{label}: key sets differ (gold-only {len(only_g)}, oracle-only {len(only_e)}) "
                       f"e.g. {sorted(only_g, key=str)[:2]} / {sorted(only_e, key=str)[:2]}")
        for k in set(gk) & set(expected):
            for gcol, ekey, tol in fields:
                if not _eq(gk[k][gcol], expected[k][ekey], tol):
                    bad.append(f"{label} {k} {gcol}: gold={gk[k][gcol]!r} oracle={expected[k][ekey]!r}")

    check("hourly", _rows(conn, "SELECT * FROM gold.hourly_sensor_pm25"), eh,
          lambda r: (r["sensor_id"], r["hour_start_utc"].astimezone(UTC)),
          [("avg_pm25", "avg", 0.0011), ("avg_pm25_excl_flatline", "excl", 0.0011),
           ("min_pm25", "mn", None), ("max_pm25", "mx", None), ("n_readings", "n", None),
           ("reading_interval_seconds", "interval", None), ("covered_seconds", "covered", None),
           ("coverage_pct", "coverage", 0.01), ("n_flatline_readings", "nflat", None),
           ("n_high_value_readings", "nhigh", None), ("local_hour_start", "local_hour", None),
           ("local_date", "local_date", None), ("location_id", "loc", None)])
    check("daily", _rows(conn, "SELECT * FROM gold.daily_location_summary"), ed,
          lambda r: (r["location_id"], r["local_date"]),
          [("avg_pm25", "avg", 0.0015), ("avg_pm25_excl_flatline", "excl", 0.0015),
           ("min_hourly_avg", "min_h", 0.0015), ("max_hourly_avg", "max_h", 0.0015),
           ("max_reading", "max_reading", None), ("n_sensors_reporting", "n_reporting", None),
           ("n_sensors_at_location", "n_at_location", None), ("n_readings", "n", None),
           ("n_location_hours", "n_hours", None), ("expected_seconds", "expected", None),
           ("covered_seconds", "covered", None), ("coverage_pct", "coverage", 0.01),
           ("is_partial_day", "partial", None), ("meets_completeness", "complete", None),
           ("n_flatline_readings", "nflat", None), ("n_high_value_readings", "nhigh", None),
           ("location_name", "name", None), ("latitude", "lat", None), ("longitude", "lon", None)])
    check("health", _rows(conn, "SELECT * FROM gold.sensor_health"), ee,
          lambda r: (r["sensor_id"], r["local_date"]),
          [("expected_interval_seconds", "interval", None), ("expected_seconds", "expected_seconds", None),
           ("expected_readings", "expected_readings", None), ("received_readings", "received", None),
           ("covered_seconds", "covered", None), ("coverage_pct", "coverage", 0.01),
           ("longest_gap_minutes", "longest_gap", 0.05), ("n_flatline_readings", "nflat", None),
           ("n_high_value_readings", "nhigh", None), ("n_long_gap_readings", "nlg", None),
           ("flatline_pct", "flat_pct", 0.01), ("first_reading_utc", "first", None),
           ("last_reading_utc", "last", None), ("is_partial_day", "partial", None),
           ("health_status", "status", None), ("location_id", "loc", None)])
    return bad, dict(hourly=len(eh), daily=len(ed), health=len(ee))


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dbname")
    a = ap.parse_args()
    with psycopg.connect(**config.db_kwargs(a.dbname)) as c:
        mismatches, n = compare(c)
    print(f"oracle compared rows: {n}")
    print(f"mismatches: {len(mismatches)}")
    for m in mismatches[:15]:
        print("  ", m)
    sys.exit(1 if mismatches else 0)
