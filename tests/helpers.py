"""Shared test helpers: build deliberately dirty Bronze data in the openaq_test database.

SAFETY: every destructive helper goes through connect_clean(), which refuses to touch any
database other than TEST_DB.
"""
import csv
import hashlib
import json
import re
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg

from pipeline import config

TEST_DB = "openaq_test"
FIXTURES = Path(__file__).parent / "fixtures"
INGESTED_AT = "2026-10-09T12:00:00Z"   # fixed "ingestion time" so FUTURE_TIMESTAMP is deterministic
FIXTURE_RUN_ID = "11111111-1111-1111-1111-111111111111"
OK_UNIT = "µg/m³"               # U+00B5 micro sign (what OpenAQ really sends)
NULL = "<NULL>"
SENSOR_IDS = list(range(1001, 1014))
ALL_TABLES = ("audit.reconciliation_checks, audit.fetch_attempts, audit.sensor_fetch_status, "
              "audit.pipeline_runs, gold.sensor_health, gold.daily_location_summary, gold.hourly_sensor_pm25, "
              "silver.bronze_classification, silver.rejected_records, silver.measurements, "
              "silver.sensors, bronze.measurements, bronze.sensors")

HARD_RULES = ["NULL_SENSOR", "MISSING_VALUE", "UNPARSEABLE_VALUE", "UNPARSEABLE_TS",
              "NEGATIVE_PM25", "FUTURE_TIMESTAMP", "ORPHAN_SENSOR",
              "WRONG_PARAMETER", "UNEXPECTED_UNIT", "INVALID_PERIOD"]


def connect_clean() -> psycopg.Connection:
    """Connection to the TEST database with every table emptied. Never the dev database."""
    assert TEST_DB != config.db_kwargs()["dbname"], "test DB must differ from the dev DB"
    conn = psycopg.connect(**config.db_kwargs(TEST_DB))
    assert conn.info.dbname == TEST_DB
    conn.execute(f"TRUNCATE {ALL_TABLES} RESTART IDENTITY")
    conn.commit()
    return conn


def _decode(cell: str):
    if cell == NULL:
        return None
    if cell == "@OK_UNIT":
        return OK_UNIT
    return re.sub(r"\\u([0-9a-fA-F]{4})", lambda m: chr(int(m.group(1), 16)), cell)


def _hash(*parts) -> str:
    return hashlib.sha256("|".join(map(str, parts)).encode()).hexdigest()


# ---------------------------------------------------------------- Bronze inserts

def insert_sensor(conn, sensor_id, **over) -> int:
    row = dict(sensor_id=str(sensor_id), sensor_name="pm25 ug/m3", parameter_id="2",
               parameter_name="pm25", units=OK_UNIT,
               location_id=str(int(sensor_id) - 500) if str(sensor_id).isdigit() else "1",
               location_name=f"Loc {sensor_id}", locality=None, provider_name="FIX",
               country_code="IN", timezone="Asia/Kolkata", latitude="28.6", longitude="77.2",
               is_monitor="True", is_mobile="False", datetime_first_utc=None,
               datetime_last_utc=None)
    row.update({k: (None if v == NULL else v) for k, v in over.items()})
    cols = list(row)
    cur = conn.execute(
        f"INSERT INTO bronze.sensors ({', '.join(cols)}, raw_json, source_url, run_id, row_hash) "
        f"VALUES ({', '.join(['%s'] * len(cols))}, %s, 'fixture', %s, %s) RETURNING bronze_id",
        [row[c] for c in cols] + [json.dumps(row), FIXTURE_RUN_ID,
                                  _hash("sensor", uuid.uuid4())])
    return cur.fetchone()[0]


def insert_sensors(conn, ids=SENSOR_IDS) -> None:
    for sid in ids:
        insert_sensor(conn, sid)
    conn.commit()


def insert_measurement(conn, case_id, sensor_id, parameter_id, value_raw, unit_raw,
                       period_from_raw, period_to_raw, interval="00:15:00",
                       ingested_at=INGESTED_AT) -> int:
    raw = dict(case_id=case_id, sensor_id=sensor_id, value=value_raw)
    cur = conn.execute(
        "INSERT INTO bronze.measurements (sensor_id, parameter_id, value_raw, unit_raw, period_label,"
        " period_interval_raw, period_from_raw, period_to_raw, has_flags_raw, raw_json, source_url,"
        " run_id, ingested_at, row_hash) VALUES (%s,%s,%s,%s,'raw',%s,%s,%s,'False',%s,%s,%s,%s,%s)"
        " RETURNING bronze_id",
        (sensor_id, parameter_id, value_raw, unit_raw, interval, period_from_raw, period_to_raw,
         json.dumps(raw), f"fixture://{case_id}", FIXTURE_RUN_ID, ingested_at, _hash("m", case_id)))
    return cur.fetchone()[0]


def read_cases() -> list[dict]:
    with open(FIXTURES / "dirty_bronze.csv", newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def load_cases(conn, cases=None) -> dict[str, int]:
    """Insert the dirty fixture in file order (file order == bronze_id order). case_id -> bronze_id"""
    ids = {}
    for c in cases or read_cases():
        ids[c["case_id"]] = insert_measurement(
            conn, c["case_id"], _decode(c["sensor_id"]), _decode(c["parameter_id"]),
            _decode(c["value_raw"]), _decode(c["unit_raw"]),
            _decode(c["period_from_raw"]), _decode(c["period_to_raw"]))
    conn.commit()
    return ids


def fmt(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def reading(conn, sensor_id, start: datetime, minutes: int, value, tag="r") -> int:
    interval = f"{minutes // 60:02d}:{minutes % 60:02d}:00"
    end = start + timedelta(minutes=minutes)
    return insert_measurement(conn, f"{tag}-{sensor_id}-{fmt(start)}", str(sensor_id), "2", str(value),
                              OK_UNIT, fmt(start), fmt(end), interval=interval)


def series(conn, sensor_id, start: datetime, minutes: int, values, tag="s") -> list[int]:
    """Back-to-back readings (no gaps), one per value."""
    return [reading(conn, sensor_id, start + timedelta(minutes=minutes * i), minutes, v, tag)
            for i, v in enumerate(values)]


# ---------------------------------------------------------------- Silver reads

def verdicts(conn) -> dict[int, dict]:
    cur = conn.execute(
        "SELECT c.bronze_id, c.classification, c.reason_code, c.duplicate_of_bronze_id, c.value_differs,"
        "       r.all_reason_codes, r.reason_detail "
        "FROM silver.bronze_classification c "
        "LEFT JOIN silver.rejected_records r USING (bronze_id)")
    cols = [d.name for d in cur.description]
    return {row[0]: dict(zip(cols, row)) for row in cur.fetchall()}


def flags(conn, ids) -> list[dict]:
    cur = conn.execute(
        "SELECT bronze_id, is_high_value, is_long_gap, is_flatline, gap_before_seconds, "
        "flatline_run_length FROM silver.measurements WHERE bronze_id = ANY(%s) ORDER BY bronze_id",
        (list(ids),))
    cols = [d.name for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def snapshot(conn) -> tuple:
    """Everything deterministic about Silver (no run ids / timestamps), for equality checks."""
    cls = conn.execute("SELECT bronze_id, classification, reason_code, duplicate_of_bronze_id, "
                       "value_differs FROM silver.bronze_classification ORDER BY 1").fetchall()
    meas = conn.execute("SELECT bronze_id, sensor_id, parameter_id, period_from_utc, period_to_utc, "
                        "pm25_ugm3, is_high_value, is_long_gap, is_flatline, gap_before_seconds, "
                        "flatline_run_length FROM silver.measurements ORDER BY 1").fetchall()
    rej = conn.execute("SELECT bronze_id, reason_code, all_reason_codes, reason_detail "
                       "FROM silver.rejected_records ORDER BY 1").fetchall()
    return cls, meas, rej


def counts(conn) -> dict:
    q = lambda sql: conn.execute(sql).fetchone()[0]
    return dict(
        bronze=q("SELECT count(*) FROM bronze.measurements"),
        classified=q("SELECT count(*) FROM silver.bronze_classification"),
        valid=q("SELECT count(*) FROM silver.bronze_classification WHERE classification='VALID'"),
        rejected=q("SELECT count(*) FROM silver.bronze_classification WHERE classification='REJECTED'"),
        duplicate=q("SELECT count(*) FROM silver.bronze_classification WHERE classification='DUPLICATE'"),
        silver_rows=q("SELECT count(*) FROM silver.measurements"),
        rejected_rows=q("SELECT count(*) FROM silver.rejected_records"),
    )
