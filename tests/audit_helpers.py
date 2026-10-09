"""Shared fixtures-as-functions for the audit tests: a fake fleet, a runner, and small DB readers."""
from datetime import date, datetime, timezone

import psycopg

from pipeline import run_all
from pipeline.openaq_client import OpenAQClient
from tests import helpers
from tests.fake_openaq import FakeSession, item, loc

UTC = timezone.utc
# SYNTHETIC test credential, not a real secret: the audit tests inject it to prove it never reaches
# any log or audit table.
KEY = "SECRET-KEY-DO-NOT-LEAK-123"
W0 = datetime(2026, 10, 2, tzinfo=UTC)
W1 = datetime(2026, 10, 5, tzinfo=UTC)
IDS = ["101", "102", "103", "104", "105", "106", "201", "202"]       # 107 is deliberately excluded
GOLD_TABLES = ("hourly_sensor_pm25", "daily_location_summary", "sensor_health")


def fleet():
    """Nine sensors covering every fetch outcome:
       101-103 CPCB with data, 104 CPCB zero rows, 105 CPCB HTTP 500, 106 CPCB inactive (skipped),
       107 CPCB not attempted (excluded), 201-202 PROV2 with data on the first day only."""
    locations = [loc(1, [101]), loc(2, [102]), loc(3, [103]), loc(4, [104]), loc(5, [105]),
                 loc(6, [106], last="2026-09-01T00:00:00Z"), loc(7, [107]),
                 loc(8, [201], provider="PROV2"), loc(9, [202], provider="PROV2")]
    sensors = {
        101: [item("2026-10-02T03:00:00Z", 15, 10), item("2026-10-03T03:00:00Z", 15, 11), item("2026-10-04T03:00:00Z", 15, 12)],
        102: [item("2026-10-02T03:00:00Z", 15, 20)],
        103: [item("2026-10-02T03:00:00Z", 15, 30), item("2026-10-04T03:00:00Z", 15, 31)],
        104: [],
        105: 500,
        201: [item("2026-10-02T03:00:00Z", 15, 40)],
        202: [item("2026-10-02T03:00:00Z", 15, 41)],
    }
    return locations, sensors


def go(api=None, **kw):
    """Run the real pipeline against the fake API in the test database. Returns (result, session, sleeps)."""
    locations, sensors = api or fleet()
    session = FakeSession(locations, sensors)
    sleeps = []
    client = OpenAQClient(KEY, session=session, sleep=sleeps.append)
    args = dict(window_from=W0, window_to=W1, dbname=helpers.TEST_DB, sensor_ids=IDS, max_failed_fraction=0.2,
                client=client, extra_secrets=[KEY])
    args.update(kw)
    return run_all.run_pipeline(**args), session, sleeps


def q(conn, sql, params=()):
    return conn.execute(sql, params).fetchall()


def one(conn, sql, params=()):
    return conn.execute(sql, params).fetchone()[0]


def rows(conn, sql, params=()):
    cur = conn.execute(sql, params)
    cols = [d.name for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def run_row(conn, run_id):
    return rows(conn, "SELECT * FROM audit.pipeline_runs WHERE run_id = %s", (run_id,))[0]


def checks(conn, run_id):
    return {r["check_name"]: r for r in rows(conn, "SELECT * FROM audit.reconciliation_checks WHERE run_id=%s", (run_id,))}


def layer_counts(conn):
    return dict(
        bronze=one(conn, "SELECT count(*) FROM bronze.measurements"),
        silver=one(conn, "SELECT count(*) FROM silver.measurements"),
        gold=sum(one(conn, f"SELECT count(*) FROM gold.{t}") for t in GOLD_TABLES))


def install_trigger(conn, table, body, when="BEFORE INSERT"):
    schema = table.split(".")[0]
    conn.execute(f"CREATE OR REPLACE FUNCTION {schema}._sabotage() RETURNS trigger LANGUAGE plpgsql AS "
                 f"$$ BEGIN {body} RETURN NEW; END $$")
    conn.execute(f"CREATE TRIGGER trg_sabotage {when} ON {table} FOR EACH ROW EXECUTE FUNCTION {schema}._sabotage()")


def remove_trigger(conn, table):
    schema = table.split(".")[0]
    conn.execute(f"DROP TRIGGER IF EXISTS trg_sabotage ON {table}")
    conn.execute(f"DROP FUNCTION IF EXISTS {schema}._sabotage()")
