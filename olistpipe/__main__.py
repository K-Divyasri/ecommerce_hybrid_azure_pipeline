"""
Command line for every pipeline step.

    python -m olistpipe generate --orders 5000 --seed 42
    python -m olistpipe seed-mysql
    python -m olistpipe simulate --day 2018-09-01 --days 7 --new-orders 15
    python -m olistpipe extract --run-date 2018-08-31          (inside docker: needs HDFS)
    python -m olistpipe spark --run-date 2018-08-31            (inside docker: needs Java)
    python -m olistpipe analytics --run-date 2018-08-31
    python -m olistpipe cosmos --run-date 2018-08-31
    python -m olistpipe export-powerbi
    python -m olistpipe watermarks
    python -m olistpipe ddl
"""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

from .config import PROJECT_DIR


def _d(s: str) -> date:
    return date.fromisoformat(s)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="olistpipe")
    sub = ap.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("generate")
    g.add_argument("--orders", type=int, default=5000)
    g.add_argument("--seed", type=int, default=42)
    g.add_argument("--out", type=Path, default=PROJECT_DIR / "data" / "raw" / "olist")
    s = sub.add_parser("seed-mysql")
    s.add_argument("--src", type=Path, default=PROJECT_DIR / "data" / "raw" / "olist")
    m = sub.add_parser("simulate")
    m.add_argument("--day", type=_d, required=True)
    m.add_argument("--new-orders", type=int, default=15,
                   help="new orders per day; history averages about 8-14 a day")
    m.add_argument("--days", type=int, default=1, help="simulate this many consecutive days")
    for name in ("extract", "spark", "analytics", "cosmos"):
        p = sub.add_parser(name)
        p.add_argument("--run-date", type=_d, required=True)
        if name == "extract":
            p.add_argument("--no-blob", action="store_true")
        if name == "spark":
            p.add_argument("--no-jdbc", action="store_true")
    e = sub.add_parser("export-powerbi")
    e.add_argument("--no-previews", action="store_true")
    sub.add_parser("watermarks")
    sub.add_parser("ddl")
    a = ap.parse_args(argv)

    from . import pipeline as pl
    if a.cmd == "generate":
        counts = pl.step_generate(a.orders, a.seed, a.out)
        for k, v in counts.items():
            print(f"{k:36s} {v:7d} rows")
        print(f"wrote CSVs to {a.out}")
    elif a.cmd == "seed-mysql":
        for k, v in pl.step_seed(a.src).items():
            print(f"[seed] {k:36s} {v:7d} rows")
    elif a.cmd == "simulate":
        from datetime import timedelta
        for i in range(a.days):
            d = a.day + timedelta(days=i)
            print(f"[simulate] {d}: {pl.step_simulate(d, a.new_orders)}")
    elif a.cmd == "extract":
        pl.step_extract(a.run_date, with_blob=not a.no_blob)
    elif a.cmd == "spark":
        return pl.step_spark(a.run_date, jdbc=not a.no_jdbc)
    elif a.cmd == "analytics":
        print(pl.step_analytics(a.run_date))
    elif a.cmd == "cosmos":
        pl.step_cosmos(a.run_date)
    elif a.cmd == "export-powerbi":
        print(pl.step_export_powerbi(previews=not a.no_previews))
    elif a.cmd == "watermarks":
        from .config import get_settings
        from .db import mysql_engine
        from .extract import CheckpointStore
        print(CheckpointStore(mysql_engine(get_settings().sqlalchemy_url)).all().to_string())
    elif a.cmd == "ddl":
        print(__import__("olistpipe.schema", fromlist=["mysql_ddl"]).mysql_ddl())
    return 0


if __name__ == "__main__":
    sys.exit(main())
