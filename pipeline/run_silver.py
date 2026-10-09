"""Run the Silver transformation (all logic lives in SQL: silver.run_silver()).

Usage:  python -m pipeline.run_silver                 # incremental: classify only new Bronze rows
        python -m pipeline.run_silver --full-refresh  # rebuild Silver from Bronze (rules changed)

One run = one transaction. If anything fails (including the reconciliation guard inside the
function), nothing is committed.
"""
import argparse
import json
import sys
import uuid

import psycopg

from pipeline import config


def run(conn, full_refresh: bool = False, run_id: uuid.UUID | None = None) -> dict:
    """Execute one Silver run on an open connection; the caller commits."""
    run_id = run_id or uuid.uuid4()
    cur = conn.execute("SELECT * FROM silver.run_silver(%s, %s)", (run_id, full_refresh))
    cols = [d.name for d in cur.description]
    return {"run_id": str(run_id), "full_refresh": full_refresh, **dict(zip(cols, cur.fetchone()))}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--full-refresh", action="store_true",
                    help="truncate Silver and re-derive everything from Bronze")
    ap.add_argument("--dbname", help="override POSTGRES_DB (used by tests)")
    a = ap.parse_args(argv)
    with psycopg.connect(**config.db_kwargs(a.dbname)) as conn:
        stats = run(conn, a.full_refresh)
        conn.commit()
    print(json.dumps(stats, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
