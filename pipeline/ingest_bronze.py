"""Ingest India PM2.5 sensors + measurements from OpenAQ v3 into the Bronze layer.

Idempotent: re-running over the same window inserts 0 new rows.
Standalone:   python -m pipeline.ingest_bronze --days 7 --end 2026-10-09T00:00:00Z
In production run_all calls run_window() so the ingest shares the pipeline's run_id and audit trail.
"""
import argparse
import hashlib
import json
import logging
import re
import sys
import uuid
from datetime import datetime, timedelta, timezone

import psycopg

from pipeline import config
from pipeline.openaq_client import FetchError, OpenAQClient

log = logging.getLogger("ingest_bronze")

INDIA_COUNTRY_ID = 9  # verified in Phase 0; /countries?iso=IN silently ignores the filter
PM25_PARAMETER_ID = 2
TS_FMT = "%Y-%m-%dT%H:%M:%SZ"
ISO_Z = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")

# The API paginates by offset, so deep pages of a long window get slow and eventually time out (HTTP 408). Each
# sensor's window is therefore fetched in slices of at most SLICE_DAYS, which keeps every request shallow.
# A slice after the first starts SLICE_OVERLAP early so a period that straddles a boundary can never fall between
# two slices whatever the API's inclusive/exclusive rule is; the overlap re-fetches identical rows, which the
# content hash skips (so no duplicate Bronze row).
SLICE_DAYS = 30
SLICE_OVERLAP = timedelta(hours=1)


def time_slices(window_from: datetime, window_to: datetime, days: int = SLICE_DAYS) -> list[tuple[datetime, datetime]]:
    """Consecutive, non-overlapping [start, end) slices of at most `days` days that exactly cover the window."""
    out, start, step = [], window_from, timedelta(days=days)
    while start < window_to:
        end = min(start + step, window_to)
        out.append((start, end))
        start = end
    return out

SENSOR_COLS = ["sensor_id", "sensor_name", "parameter_id", "parameter_name", "units",
               "location_id", "location_name", "locality", "provider_name", "country_code",
               "timezone", "latitude", "longitude", "is_monitor", "is_mobile",
               "datetime_first_utc", "datetime_last_utc",
               "raw_json", "source_url", "run_id", "row_hash"]
# datetime_first/last move on every run: stored, but kept OUT of the hash.
SENSOR_HASH_COLS = SENSOR_COLS[:15]

MEAS_COLS = ["sensor_id", "parameter_id", "value_raw", "unit_raw", "period_label",
             "period_interval_raw", "period_from_raw", "period_to_raw", "has_flags_raw",
             "raw_json", "source_url", "run_id", "row_hash"]


def txt(v):
    """Raw value -> TEXT, preserving SQL NULL for JSON null (never inventing '')."""
    return None if v is None else str(v)


def canonical(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def fetch_india_pm25_sensors(client: OpenAQClient):
    """Return [(location, sensor, url)] for PM2.5 sensors in India, sorted by sensor id."""
    out = []
    for results, url in client.pages("locations", {"countries_id": INDIA_COUNTRY_ID,
                                                   "parameters_id": PM25_PARAMETER_ID},
                                     ctx={"parameter_id": PM25_PARAMETER_ID}):
        for loc in results:
            # Phase 0 lesson: assert the RESULT, not just that the call succeeded.
            if loc["country"]["code"] != "IN":
                raise RuntimeError(f"non-India location returned: {loc['id']} {loc['country']}")
            for s in loc["sensors"]:
                if s["parameter"]["id"] == PM25_PARAMETER_ID:
                    out.append((loc, s, url))
    # The API gives no ordering guarantee; sort so --limit-sensors and reruns are reproducible.
    out.sort(key=lambda t: t[1]["id"])
    return out


def sensor_row(loc, sensor, url, run_id) -> dict:
    row = {
        "sensor_id": txt(sensor["id"]), "sensor_name": txt(sensor.get("name")),
        "parameter_id": txt(sensor["parameter"]["id"]),
        "parameter_name": txt(sensor["parameter"]["name"]),
        "units": txt(sensor["parameter"]["units"]),
        "location_id": txt(loc["id"]), "location_name": txt(loc.get("name")),
        "locality": txt(loc.get("locality")),
        "provider_name": txt((loc.get("provider") or {}).get("name")),
        "country_code": txt((loc.get("country") or {}).get("code")),
        "timezone": txt(loc.get("timezone")),
        "latitude": txt((loc.get("coordinates") or {}).get("latitude")),
        "longitude": txt((loc.get("coordinates") or {}).get("longitude")),
        "is_monitor": txt(loc.get("isMonitor")), "is_mobile": txt(loc.get("isMobile")),
        "datetime_first_utc": txt((loc.get("datetimeFirst") or {}).get("utc")),
        "datetime_last_utc": txt((loc.get("datetimeLast") or {}).get("utc")),
    }
    row["raw_json"] = canonical({**{k: v for k, v in loc.items() if k != "sensors"},
                                 "sensor": sensor})
    row["source_url"] = url
    row["run_id"] = str(run_id)
    row["row_hash"] = sha256("\x1f".join(row[c] or "\x00" for c in SENSOR_HASH_COLS))
    return row


def measurement_row(sensor_id, item, url, run_id) -> dict:
    p = item.get("period") or {}
    row = {
        "sensor_id": txt(sensor_id),
        "parameter_id": txt((item.get("parameter") or {}).get("id")),
        "value_raw": txt(item.get("value")),
        "unit_raw": txt((item.get("parameter") or {}).get("units")),
        "period_label": txt(p.get("label")),
        "period_interval_raw": txt(p.get("interval")),
        "period_from_raw": txt((p.get("datetimeFrom") or {}).get("utc")),
        "period_to_raw": txt((p.get("datetimeTo") or {}).get("utc")),
        "has_flags_raw": txt((item.get("flagInfo") or {}).get("hasFlags")),
        "raw_json": canonical(item),
        "source_url": url,
        "run_id": str(run_id),
    }
    # Hash = identity + full payload. NOT run_id / ingested_at / url (those change per run).
    row["row_hash"] = sha256(f"{sensor_id}|{row['raw_json']}")
    return row


def load_chunk(conn, table: str, cols: list[str], rows: list[dict]) -> tuple[int, int]:
    """COPY rows into a staging table, then INSERT ... ON CONFLICT DO NOTHING.
    Returns (inserted, skipped_because_already_present)."""
    if not rows:
        return 0, 0
    stage = f"_stage_{table.split('.')[1]}"
    col_list = ", ".join(cols)
    with conn.cursor() as cur:
        cur.execute(f"CREATE TEMP TABLE IF NOT EXISTS {stage} ON COMMIT DELETE ROWS AS "
                    f"SELECT {col_list} FROM {table} WITH NO DATA")
        with cur.copy(f"COPY {stage} ({col_list}) FROM STDIN") as cp:
            for r in rows:
                cp.write_row([r[c] for c in cols])
        cur.execute(f"INSERT INTO {table} ({col_list}) SELECT {col_list} FROM {stage} "
                    f"ON CONFLICT (row_hash) DO NOTHING")
        inserted = cur.rowcount
    conn.commit()
    return inserted, len(rows) - inserted


def run_window(conn, client: OpenAQClient, run_id, window_from: datetime, window_to: datetime, *,
               limit_sensors: int | None = None, sensor_ids: list[str] | None = None, audit=None) -> dict:
    """Ingest [window_from, window_to) into Bronze under `run_id`.

    Each sensor is fetched in time slices (see time_slices); a slice that fails after all retries fails the sensor and
    the sensor's remaining slices are not requested. Rows already present (same hash) are skipped.

    Only FetchError (a request that could not be completed even after retries) is treated as a per-sensor
    failure: it is recorded and the run continues. Anything else - a database error, a bug - propagates
    and fails the run. If `audit` is given, every sensor ends with a row in audit.sensor_fetch_status."""
    wf, wt = window_from.strftime(TS_FMT), window_to.strftime(TS_FMT)
    stats = {"run_id": str(run_id), "window_from": wf, "window_to": wt,
             "requested_sensors": 0, "skipped_inactive": 0, "not_attempted": 0, "attempted_sensors": 0,
             "fetched_sensors": 0, "zero_row_sensors": 0, "failed_sensors": [],
             "sensor_rows_fetched": 0, "sensor_rows_inserted": 0,
             "meas_rows_fetched": 0, "meas_rows_inserted": 0, "meas_rows_skipped": 0,
             "observed_from": None, "observed_to": None}

    found = fetch_india_pm25_sensors(client)
    stats["requested_sensors"] = len(found)
    ins, _ = load_chunk(conn, "bronze.sensors", SENSOR_COLS,
                        [sensor_row(l, s, u, run_id) for l, s, u in found])
    stats["sensor_rows_fetched"], stats["sensor_rows_inserted"] = len(found), ins

    # Decide every sensor's fate up front, so the audit trail accounts for ALL discovered sensors.
    todo, seed = [], []
    for loc, s, _ in found:
        sid = s["id"]
        base = dict(sensor_id=sid, location_id=loc["id"], provider_name=(loc.get("provider") or {}).get("name"))
        last = (loc.get("datetimeLast") or {}).get("utc")
        if sensor_ids and str(sid) not in sensor_ids:
            seed.append({**base, "status": "NOT_ATTEMPTED", "reason": "excluded by --sensor-id"})
            stats["not_attempted"] += 1
        elif last and last < wf:
            seed.append({**base, "status": "SKIPPED_INACTIVE",
                         "reason": f"location datetimeLast {last} is before the window start {wf}"})
            stats["skipped_inactive"] += 1
        else:
            todo.append(sid)
            seed.append({**base, "status": "NOT_ATTEMPTED", "reason": "queued; the run ended before reaching it"})
    if limit_sensors:
        for sid in todo[limit_sensors:]:
            for row in seed:
                if row["sensor_id"] == sid:
                    row["reason"] = "beyond --limit-sensors"
            stats["not_attempted"] += 1
        todo = todo[:limit_sensors]
    if audit:
        audit.seed_sensor_status(seed)

    slices = time_slices(window_from, window_to)
    stats["slices"] = len(slices)
    observed_from, observed_to = None, None
    for n, sid in enumerate(todo, 1):
        sensor = dict(rows=0, pages=0, first=None, last=None)
        sl = (window_from, window_to)
        try:
            for s_start, s_end in slices:
                # fetch window for this slice (the first slice starts exactly at window_from)
                sl = (max(window_from, s_start - SLICE_OVERLAP), s_end)
                for results, url in client.pages(f"sensors/{sid}/measurements",
                                                 {"datetime_from": sl[0].strftime(TS_FMT), "datetime_to": sl[1].strftime(TS_FMT)},
                                                 ctx={"sensor_id": sid, "parameter_id": PM25_PARAMETER_ID,
                                                      "window_from": sl[0], "window_to": sl[1]}):
                    rows = [measurement_row(sid, it, url, run_id) for it in results]
                    ins, skip = load_chunk(conn, "bronze.measurements", MEAS_COLS, rows)
                    stats["meas_rows_fetched"] += len(rows)
                    stats["meas_rows_inserted"] += ins
                    stats["meas_rows_skipped"] += skip
                    sensor["rows"] += len(rows)
                    sensor["pages"] += 1
                    for r in rows:
                        if r["period_from_raw"] and ISO_Z.match(r["period_from_raw"]):
                            sensor["first"] = min(sensor["first"] or r["period_from_raw"], r["period_from_raw"])
                        if r["period_to_raw"] and ISO_Z.match(r["period_to_raw"]):
                            sensor["last"] = max(sensor["last"] or r["period_to_raw"], r["period_to_raw"])
            stats["attempted_sensors"] += 1
            if sensor["rows"]:
                stats["fetched_sensors"] += 1
            else:
                stats["zero_row_sensors"] += 1
            if sensor["first"]:
                observed_from = min(observed_from or sensor["first"], sensor["first"])
                observed_to = max(observed_to or sensor["last"], sensor["last"])
            if audit:
                audit.sensor_done(sid, status="FETCHED" if sensor["rows"] else "ZERO_ROWS",
                                  reason=None if sensor["rows"] else "request succeeded; the source returned no rows",
                                  rows=sensor["rows"], pages=sensor["pages"],
                                  first=sensor["first"], last=sensor["last"])
        except FetchError as exc:       # recorded, and the run goes on; the failure RATE is reconciled later
            stats["attempted_sensors"] += 1
            stats["failed_sensors"].append(str(sid))
            reason = f"{exc} [slice {sl[0].strftime(TS_FMT)} -> {sl[1].strftime(TS_FMT)}]"
            log.error("sensor %s failed: %s", sid, reason)
            if audit:
                audit.sensor_done(sid, status="FAILED", reason=reason, rows=sensor["rows"], pages=sensor["pages"],
                                  first=sensor["first"], last=sensor["last"],
                                  http_status=exc.http_status, error_type=exc.outcome, error_message=str(exc))
        if n % 25 == 0:
            log.info("progress %s/%s sensors, %s measurement rows fetched", n, len(todo), stats["meas_rows_fetched"])
    stats["observed_from"], stats["observed_to"] = observed_from, observed_to
    return stats


def run(days: int, end: datetime, limit_sensors: int | None, sensor_ids: list[str] | None) -> dict:
    """Standalone ingest (no audit trail). Production runs go through pipeline.run_all."""
    run_id = uuid.uuid4()
    client = OpenAQClient(config.api_key())
    with psycopg.connect(**config.db_kwargs()) as conn:
        return run_window(conn, client, run_id, end - timedelta(days=days), end,
                          limit_sensors=limit_sensors, sensor_ids=sensor_ids)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--days", type=int, default=90)
    ap.add_argument("--end", help="window end, ISO UTC (default: now, floored to the minute)")
    ap.add_argument("--limit-sensors", type=int)
    ap.add_argument("--sensor-id", action="append", dest="sensor_ids")
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    end = (datetime.fromisoformat(a.end.replace("Z", "+00:00")) if a.end
           else datetime.now(timezone.utc).replace(second=0, microsecond=0))
    stats = run(a.days, end, a.limit_sensors, a.sensor_ids)
    print(json.dumps(stats, indent=2))
    return 1 if stats["failed_sensors"] else 0


if __name__ == "__main__":
    sys.exit(main())
