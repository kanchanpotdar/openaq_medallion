"""Build the Gold layer from Silver (all logic lives in SQL: gold.run_gold()).

Usage:  python -m pipeline.run_gold
        python -m pipeline.run_gold --completeness 80 --stuck 95

Every run recomputes Gold from Silver and applies only the differences (idempotent: an
unchanged rerun writes 0 rows). One run = one transaction; any reconciliation-guard failure
rolls everything back.
"""
import argparse
import json
import sys
import uuid
from decimal import Decimal

import psycopg

from pipeline import config

# CLI flag -> SQL parameter. Only flags the user passes are sent, so the approved defaults
# live in exactly one place: the SQL function signature.
THRESHOLD_PARAMS = {
    "completeness": "p_completeness_pct",
    "stuck": "p_stuck_flatline_pct",
    "low_coverage": "p_low_coverage_pct",
    "partial_coverage": "p_partial_coverage_pct",
}


def run(conn, run_id: uuid.UUID | None = None, **thresholds) -> dict:
    """Execute one Gold build on an open connection; the caller commits.
    thresholds: any of p_completeness_pct, p_stuck_flatline_pct, p_low_coverage_pct,
    p_partial_coverage_pct, p_horizon_from, p_horizon_to."""
    run_id = run_id or uuid.uuid4()
    # psycopg sends float as float8, which Postgres will not implicitly cast to the numeric parameters.
    thresholds = {k: Decimal(str(v)) if isinstance(v, float) else v for k, v in thresholds.items()}
    named =", ".join(f"{k} => %s" for k in thresholds)
    sql = f"SELECT * FROM gold.run_gold(p_run_id => %s{', ' + named if named else ''})"
    cur = conn.execute(sql, [run_id, *thresholds.values()])
    cols = [d.name for d in cur.description]
    row = dict(zip(cols, cur.fetchone()))
    for k in ("out_horizon_from", "out_horizon_to"):
        row[k] = row[k].isoformat() if row[k] else None
    return {"run_id": str(run_id), **row}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dbname", help="override POSTGRES_DB (used by tests)")
    ap.add_argument("--completeness", type=float, help="daily meets_completeness cutoff, %% (default 75)")
    ap.add_argument("--stuck", type=float, help="STUCK if flatline_pct >= this (default 90)")
    ap.add_argument("--low-coverage", dest="low_coverage", type=float, help="LOW_COVERAGE below this %% (default 50)")
    ap.add_argument("--partial-coverage", dest="partial_coverage", type=float,
                    help="PARTIAL below this %% (default 90)")
    a = ap.parse_args(argv)
    overrides = {sql: getattr(a, flag) for flag, sql in THRESHOLD_PARAMS.items()
                 if getattr(a, flag) is not None}
    with psycopg.connect(**config.db_kwargs(a.dbname)) as conn:
        stats = run(conn, **overrides)
        conn.commit()
    print(json.dumps(stats, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
