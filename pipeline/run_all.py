"""Run the whole pipeline - Bronze, Silver, Gold, reconciliation - under ONE run_id with a durable audit trail.

    python -m pipeline.run_all --days 7 --end 2026-10-09T00:00:00Z

Transaction scopes
  * Bronze        append-only raw data, committed chunk by chunk (never rolled back).
  * Silver+Gold+reconciliation   ONE transaction. A failed critical check rolls Silver and Gold back.
  * audit.*       written on a separate autocommit connection, so a failed run still leaves its evidence.

A run is SUCCESS only if every stage finished and no CRITICAL check failed. Nothing is swallowed: a failure is
recorded in audit.pipeline_runs and then re-raised as PipelineFailed (original exception chained).
"""
import argparse
import json
import logging
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

import psycopg

from pipeline import audit, config, ingest_bronze, run_gold, run_silver
from pipeline.audit import PipelineBusy, PipelineFailed
from pipeline.openaq_client import OpenAQClient

log = logging.getLogger("run_all")
PIPELINE_NAME = "openaq_india_pm25"
DEFAULT_MAX_FAILED_FRACTION = 0.05


def _iso(v):
    return v.isoformat() if hasattr(v, "isoformat") else v


def effective_horizon(conn, window_from, window_to):
    """The explicit window, widened ONLY as far as needed to contain Silver data. The API returns periods that
    overlap the window edges (e.g. one starting 30 minutes before window_from); Gold requires a horizon that
    covers every reading, so we widen here and keep Gold's own safety check strict."""
    lo, hi = conn.execute("SELECT min(period_from_utc), max(period_to_utc) FROM silver.measurements").fetchone()
    return min(window_from, lo) if lo else window_from, max(window_to, hi) if hi else window_to


def run_pipeline(*, window_from: datetime, window_to: datetime, dbname: str | None = None,
                 limit_sensors: int | None = None, sensor_ids: list[str] | None = None,
                 max_failed_fraction: float = DEFAULT_MAX_FAILED_FRACTION, run_id: uuid.UUID | None = None,
                 client=None, ingest_fn=None, sleep=time.sleep, pipeline_name: str = PIPELINE_NAME,
                 extra_secrets=()) -> dict:
    """Run one audited pipeline execution. Returns the summary; raises PipelineFailed / PipelineBusy."""
    run_id = run_id or uuid.uuid4()
    audit_conn = audit.connect_audit(dbname)
    if not audit.acquire_run_lock(audit_conn):
        audit_conn.close()
        raise PipelineBusy("another run_all is already running (advisory lock held)")
    conn = None
    stage = "start"
    secrets = list(extra_secrets)
    try:
        abandoned = audit.abandon_orphans(audit_conn)
        if abandoned:
            log.warning("marked %s abandoned RUNNING run(s) as FAILED", abandoned)
        audit.start_run(audit_conn, run_id, pipeline_name, window_from, window_to, {
            "limit_sensors": limit_sensors, "sensor_ids": sensor_ids,
            "max_failed_fraction": max_failed_fraction, "abandoned_runs_cleaned": abandoned})
        log.info("run %s started: window [%s, %s)", run_id, window_from, window_to)

        conn = psycopg.connect(**config.db_kwargs(dbname))
        if client is None:
            key = config.api_key()
            secrets.append(key)
            client = OpenAQClient(key, sleep=sleep)
        writer = audit.AuditWriter(audit_conn, run_id, secrets=secrets)
        client.on_attempt = writer.attempt

        # ---------------------------------------------------------------- 1. Bronze
        stage = "bronze"
        audit.update_run(audit_conn, run_id, stage=stage)
        bronze = (ingest_fn or ingest_bronze.run_window)(
            conn, client, run_id, window_from, window_to,
            limit_sensors=limit_sensors, sensor_ids=sensor_ids, audit=writer)
        conn.commit()
        successful = bronze["fetched_sensors"] + bronze["zero_row_sensors"]
        run_stats = dict(
            requested_sensor_count=bronze["requested_sensors"], skipped_inactive_count=bronze["skipped_inactive"],
            attempted_sensor_count=bronze["attempted_sensors"], successful_sensor_count=successful,
            failed_sensor_count=len(bronze["failed_sensors"]),
            bronze_rows_fetched=bronze["meas_rows_fetched"], bronze_rows_inserted=bronze["meas_rows_inserted"],
            bronze_rows_skipped=bronze["meas_rows_skipped"])
        audit.update_run(audit_conn, run_id, **run_stats,
                         observed_from_utc=bronze["observed_from"], observed_to_utc=bronze["observed_to"])

        # ---------------------------------------------------------------- 2-4. Silver + Gold + reconciliation (one transaction)
        stage = "silver"
        audit.update_run(audit_conn, run_id, stage=stage)
        silver = run_silver.run(conn, run_id=run_id)

        h_from, h_to = effective_horizon(conn, window_from, window_to)
        stage = "gold"
        audit.update_run(audit_conn, run_id, stage=stage,
                         effective_horizon_from_utc=h_from, effective_horizon_to_utc=h_to)
        gold = run_gold.run(conn, run_id=run_id, p_horizon_from=h_from, p_horizon_to=h_to)

        stage = "reconcile"
        audit.update_run(audit_conn, run_id, stage=stage)
        gold_written = sum(gold[f"out_{t}_{k}"] for t in ("hourly", "daily", "health") for k in ("inserted", "updated"))
        gold_deleted = sum(gold[f"out_{t}_deleted"] for t in ("hourly", "daily", "health"))
        live = dict(run_stats, silver_classified=silver["out_classified"], gold_rows_written=gold_written,
                    effective_horizon_from_utc=h_from.isoformat(), effective_horizon_to_utc=h_to.isoformat(),
                    max_failed_fraction=max_failed_fraction)
        checks = audit.reconcile(conn, run_id, live)
        failed = audit.critical_failures(checks)
        details = {"silver": {k: v for k, v in silver.items() if k != "run_id"},
                   "gold": {k: (_iso(v)) for k, v in gold.items() if k != "run_id"},
                   "bronze_failed_sensors": bronze["failed_sensors"]}

        if failed:
            conn.rollback()                                  # Silver + Gold from this run are undone
            audit.record_checks(audit_conn, run_id, checks)  # ...but the evidence is kept
            message = "critical reconciliation failed: " + "; ".join(
                f"{c['check_name']} (expected {c['expected_value']}, actual {c['actual_value']})" for c in failed)
            audit.finish_run(audit_conn, run_id, "FAILED", message, details=details)
            log.error("run %s FAILED at reconcile: %s", run_id, message)
            raise PipelineFailed(message, _result(audit_conn, run_id))
        conn.commit()
        audit.record_checks(audit_conn, run_id, checks)
        audit.finish_run(
            audit_conn, run_id, "SUCCESS", silver_valid=silver["out_valid"], silver_rejected=silver["out_rejected"],
            silver_duplicate=silver["out_duplicate"], gold_hourly_rows=gold["out_hourly_total"],
            gold_daily_rows=gold["out_daily_total"], gold_health_rows=gold["out_health_total"],
            gold_rows_written=gold_written, gold_rows_deleted=gold_deleted, details=details)
        log.info("run %s SUCCESS", run_id)
        return _result(audit_conn, run_id)

    except PipelineFailed:
        raise
    except BaseException as exc:                             # includes KeyboardInterrupt: never leave RUNNING behind
        message = audit.redact(f"{type(exc).__name__} during {stage}: {exc}", secrets, 1000)
        try:
            if conn is not None:
                conn.rollback()
            guard = audit.guard_check_from_error(str(exc))
            if guard:
                audit.record_checks(audit_conn, run_id, [guard])
            audit.finish_run(audit_conn, run_id, "FAILED", audit.redact(message, secrets, 1000), stage=stage)
        except Exception:                                    # pragma: no cover - audit itself is broken
            log.exception("could not record the failure of run %s", run_id)
        log.error("run %s FAILED during %s: %s", run_id, stage, exc)
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        raise PipelineFailed(message, _result_safe(audit_conn, run_id)) from exc
    finally:
        if conn is not None:
            conn.close()
        try:
            audit.release_run_lock(audit_conn)
        finally:
            audit_conn.close()


def _result(conn, run_id) -> dict:
    cur = conn.execute("SELECT * FROM audit.pipeline_runs WHERE run_id = %s", (run_id,))
    cols = [d.name for d in cur.description]
    run = dict(zip(cols, cur.fetchone()))
    chk = conn.execute("SELECT status, count(*) FROM audit.reconciliation_checks WHERE run_id = %s GROUP BY 1",
                       (run_id,)).fetchall()
    run["checks"] = {status: n for status, n in chk}
    return run


def _result_safe(conn, run_id):
    try:
        return _result(conn, run_id)
    except Exception:                                        # pragma: no cover
        return {"run_id": run_id}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=int, default=7, help="window length (default 7)")
    ap.add_argument("--end", help="window end, ISO UTC (default: now, floored to the minute). Pin it to reproduce a window.")
    ap.add_argument("--limit-sensors", type=int)
    ap.add_argument("--sensor-id", action="append", dest="sensor_ids")
    ap.add_argument("--max-failed-fraction", type=float, default=DEFAULT_MAX_FAILED_FRACTION,
                    help="fail the run if more than this fraction of attempted sensors fail to fetch (default 0.05)")
    ap.add_argument("--dbname", help="override POSTGRES_DB (used by tests)")
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    end = (datetime.fromisoformat(a.end.replace("Z", "+00:00")) if a.end
           else datetime.now(timezone.utc).replace(second=0, microsecond=0))
    try:
        result = run_pipeline(window_from=end - timedelta(days=a.days), window_to=end, dbname=a.dbname,
                              limit_sensors=a.limit_sensors, sensor_ids=a.sensor_ids,
                              max_failed_fraction=a.max_failed_fraction)
    except PipelineBusy as exc:
        print(f"BUSY: {exc}", file=sys.stderr)
        return 3
    except PipelineFailed as exc:
        print(json.dumps(exc.result, indent=2, default=str))
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
