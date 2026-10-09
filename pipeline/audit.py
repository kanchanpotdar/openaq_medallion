"""Audit trail writer. Everything here runs on a dedicated AUTOCOMMIT connection, so evidence of a
failed run (fetch attempts, failed checks, the FAILED status) survives the rollback of the pipeline's
own transaction.

Secrets: the API key lives only in a request header and is never part of a stored URL. As defence in
depth every stored error message is passed through redact() with the known secrets.
"""
import re
from collections import defaultdict
from datetime import datetime, timezone

import psycopg
from psycopg.types.json import Jsonb

from pipeline import config

RUN_LOCK_KEY = "audit.run_all"
RUN_COLUMNS = {
    "stage", "effective_horizon_from_utc", "effective_horizon_to_utc", "observed_from_utc", "observed_to_utc",
    "requested_sensor_count", "skipped_inactive_count", "attempted_sensor_count", "successful_sensor_count",
    "failed_sensor_count", "bronze_rows_fetched", "bronze_rows_inserted", "bronze_rows_skipped",
    "silver_valid", "silver_rejected", "silver_duplicate", "gold_hourly_rows", "gold_daily_rows",
    "gold_health_rows", "gold_rows_written", "gold_rows_deleted", "details",
}


class PipelineBusy(RuntimeError):
    """Another run_all is already executing."""


class PipelineFailed(RuntimeError):
    """The run finished FAILED. `result` carries the audit summary; __cause__ the original error if any."""

    def __init__(self, message, result):
        super().__init__(message)
        self.result = result


def redact(text, secrets=(), limit=500):
    if text is None:
        return None
    text = str(text)
    for s in secrets:
        if s:
            text = text.replace(s, "***REDACTED***")
    return text[:limit]


def connect_audit(dbname=None) -> psycopg.Connection:
    return psycopg.connect(**config.db_kwargs(dbname), autocommit=True)


# ---------------------------------------------------------------- run lifecycle

def acquire_run_lock(conn) -> bool:
    """Session-level advisory lock held for the whole run: the first line of concurrent-run protection."""
    return conn.execute("SELECT pg_try_advisory_lock(hashtext(%s))", (RUN_LOCK_KEY,)).fetchone()[0]


def release_run_lock(conn) -> None:
    conn.execute("SELECT pg_advisory_unlock(hashtext(%s))", (RUN_LOCK_KEY,))


def abandon_orphans(conn) -> int:
    """We hold the run lock, so any row still RUNNING belongs to a process that died."""
    cur = conn.execute(
        "UPDATE audit.pipeline_runs SET status = 'FAILED', finished_at = now(), "
        "error_message = 'abandoned: the process ended without finishing the run' WHERE status = 'RUNNING'")
    return cur.rowcount


def start_run(conn, run_id, pipeline_name, window_from, window_to, parameters) -> None:
    conn.execute(
        "INSERT INTO audit.pipeline_runs (run_id, pipeline_name, status, stage, window_from_utc, window_to_utc, parameters) "
        "VALUES (%s, %s, 'RUNNING', 'start', %s, %s, %s)",
        (run_id, pipeline_name, window_from, window_to, Jsonb(parameters)))


def update_run(conn, run_id, **fields) -> None:
    unknown = set(fields) - RUN_COLUMNS
    if unknown:
        raise ValueError(f"unknown pipeline_runs columns: {sorted(unknown)}")
    if not fields:
        return
    sets = ", ".join(f"{k} = %s" for k in fields)
    vals = [Jsonb(v) if k == "details" else v for k, v in fields.items()]
    conn.execute(f"UPDATE audit.pipeline_runs SET {sets} WHERE run_id = %s", [*vals, run_id])


def finish_run(conn, run_id, status, error_message=None, **fields) -> None:
    if status not in ("SUCCESS", "FAILED"):
        raise ValueError(status)
    if status == "FAILED" and not error_message:
        raise ValueError("a FAILED run needs an error_message")
    update_run(conn, run_id, **({"stage": "done"} if status == "SUCCESS" else {}), **fields)
    conn.execute("UPDATE audit.pipeline_runs SET status = %s, finished_at = now(), error_message = %s WHERE run_id = %s",
                 (status, error_message, run_id))


def record_checks(conn, run_id, checks) -> None:
    for c in checks:
        conn.execute(
            "INSERT INTO audit.reconciliation_checks (run_id, check_name, severity, expected_value, actual_value, status, details) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (run_id, check_name) DO UPDATE SET severity = EXCLUDED.severity, "
            "expected_value = EXCLUDED.expected_value, actual_value = EXCLUDED.actual_value, "
            "status = EXCLUDED.status, details = EXCLUDED.details, checked_at = now()",
            (run_id, c["check_name"], c["severity"], c.get("expected_value"), c.get("actual_value"),
             c["status"], c.get("details")))


def reconcile(conn, run_id, stats: dict | None = None) -> list[dict]:
    """Run audit.reconcile() on `conn` (inside the caller's transaction) and return the check rows."""
    cur = conn.execute("SELECT * FROM audit.reconcile(%s, %s)", (run_id, Jsonb(stats or {})))
    cols = [d.name for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def critical_failures(checks) -> list[dict]:
    return [c for c in checks if c["status"] == "FAIL" and c["severity"] == "CRITICAL"]


_GUARD = re.compile(r"^(G\d) failed|^(silver reconciliation failed)")


def guard_check_from_error(message: str):
    """Turn a Silver/Gold in-database guard exception into a durable check row (or None)."""
    m = _GUARD.match(message or "")
    if not m:
        return None
    name = f"gold_guard_{m.group(1)}" if m.group(1) else "silver_guard"
    return dict(check_name=name, severity="CRITICAL", expected_value=None, actual_value=None,
                status="FAIL", details=message[:500])


# ---------------------------------------------------------------- fetch audit

class AuditWriter:
    """Receives HTTP attempt records from the client and per-sensor outcomes from the ingest."""

    def __init__(self, conn, run_id, secrets=()):
        self.conn, self.run_id, self.secrets = conn, run_id, tuple(secrets)
        self._attempts = defaultdict(int)
        self._last_status = {}

    def attempt(self, a: dict) -> None:
        ctx = a.get("ctx") or {}
        sid = ctx.get("sensor_id")
        if sid is not None:
            self._attempts[sid] += 1
            self._last_status[sid] = a["http_status"]
        duration = int((a["finished_at"] - a["started_at"]).total_seconds() * 1000)
        self.conn.execute(
            "INSERT INTO audit.fetch_attempts (run_id, endpoint, sensor_id, parameter_id, page, request_window_from, "
            "request_window_to, request_url, attempt_no, started_at, finished_at, duration_ms, outcome, http_status, "
            "rows_returned, error_type, error_message, backoff_seconds, is_final) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (self.run_id, redact(a["endpoint"], self.secrets, 200), sid, ctx.get("parameter_id"), ctx.get("page"),
             ctx.get("window_from"), ctx.get("window_to"), redact(a["url"], self.secrets, 2000), a["attempt_no"],
             a["started_at"], a["finished_at"], duration, a["outcome"], a["http_status"], a["rows_returned"],
             redact(a["error_type"], self.secrets, 200), redact(a["error_message"], self.secrets),
             a["backoff_seconds"], a["is_final"]))

    def seed_sensor_status(self, rows) -> None:
        for r in rows:
            self.conn.execute(
                "INSERT INTO audit.sensor_fetch_status (run_id, sensor_id, location_id, provider_name, status, reason) "
                "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (run_id, sensor_id) DO NOTHING",
                (self.run_id, r["sensor_id"], r["location_id"], r["provider_name"], r["status"],
                 redact(r.get("reason"), self.secrets)))

    def sensor_done(self, sensor_id, *, status, reason=None, rows=0, pages=0, first=None, last=None,
                    http_status=None, error_type=None, error_message=None) -> None:
        self.conn.execute(
            "UPDATE audit.sensor_fetch_status SET status=%s, reason=%s, pages=%s, http_attempts=%s, rows_fetched=%s, "
            "first_period_from_utc=%s, last_period_to_utc=%s, last_http_status=%s, error_type=%s, error_message=%s, "
            "updated_at=now() WHERE run_id=%s AND sensor_id=%s",
            (status, redact(reason, self.secrets), pages, self._attempts[sensor_id], rows, first, last,
             http_status if http_status is not None else self._last_status.get(sensor_id),
             redact(error_type, self.secrets, 200), redact(error_message, self.secrets), self.run_id, sensor_id))


def utcnow():
    return datetime.now(timezone.utc)
