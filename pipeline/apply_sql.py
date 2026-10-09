"""Apply db/*.sql in filename order. Every file is written to be re-runnable."""
import sys

import psycopg

from pipeline.config import ROOT, db_kwargs


def apply_all(sql_dir=ROOT / "db", dbname: str | None = None) -> list[str]:
    applied = []
    with psycopg.connect(**db_kwargs(dbname), autocommit=True) as conn:
        for path in sorted(sql_dir.glob("*.sql")):
            conn.execute(path.read_text(encoding="utf-8"))
            applied.append(path.name)
    return applied


if __name__ == "__main__":
    for name in apply_all():
        print("applied", name)
    sys.exit(0)
