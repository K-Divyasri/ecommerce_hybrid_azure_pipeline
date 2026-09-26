"""
The pipeline steps as plain functions. The Airflow DAG and the CLI both call these, so
everything the DAG does can also be run and debugged by hand, one step at a time.
"""
from __future__ import annotations

import os
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

from .config import PROJECT_DIR, Settings, get_settings
from .db import mysql_engine


def _engines(s: Settings):
    return mysql_engine(s.sqlalchemy_url), mysql_engine(s.analytics_url)


def raw_store(s: Settings):
    """RAW_STORE=local:<folder> swaps HDFS for a folder (handy for debugging)."""
    from .storage import HdfsStore, LocalStore
    spec = os.environ.get("RAW_STORE", "hdfs")
    if spec.startswith("local:"):
        return LocalStore(spec.split(":", 1)[1])
    return HdfsStore(s.webhdfs_url, s.hdfs_user)


def blob_store(s: Settings):
    from .storage import AzureBlobStore
    return AzureBlobStore(s.blob_connection_string, s.blob_container)


def cosmos_store(s: Settings):
    if os.environ.get("COSMOS_FAKE") == "1":
        from .cosmos_store import FakeCosmosStore
        return FakeCosmosStore()
    from .cosmos_store import CosmosStore
    return CosmosStore(s.cosmos_endpoint, s.cosmos_key, s.cosmos_database)


# ------------------------------------------------------------------------------ steps
def step_generate(n_orders: int, seed: int, out: Path) -> dict:
    from .generator import generate, write_csvs
    return write_csvs(generate(n_orders=n_orders, seed=seed), out)


def step_seed(src: Path) -> dict:
    from .generator import read_csvs
    from .loader import seed
    eng, _ = _engines(get_settings())
    return seed(eng, read_csvs(src))


def step_simulate(day: date, new_orders: int) -> dict:
    from .simulator import simulate_day
    eng, _ = _engines(get_settings())
    return simulate_day(eng, day, new_orders=new_orders)


def step_extract(run_date: date, tables: list[str] | None = None, with_blob: bool = True) -> list:
    from .extract import CheckpointStore, run_extract
    s = get_settings()
    eng, _ = _engines(s)
    return run_extract(eng, CheckpointStore(eng), raw_store(s), blob_store(s) if with_blob else None,
                       s.raw_root, run_date, tables)


def step_spark(run_date: date, jdbc: bool = True) -> int:
    """Run the Spark job in a child process: the JVM starts and exits with it, and its
    exit code (2 = quality gate failed) becomes this step's result."""
    cmd = [sys.executable, "-m", "spark_jobs.build_gold", "--run-date", run_date.isoformat()]
    if jdbc:
        cmd.append("--jdbc")
    env = dict(os.environ, PYTHONPATH=str(PROJECT_DIR) + os.pathsep + os.environ.get("PYTHONPATH", ""))
    return subprocess.run(cmd, cwd=PROJECT_DIR, env=env).returncode


def step_analytics(run_date: date) -> dict:
    """RFM + revenue anomalies from the serving tables, written back to analytics.
    as_of = the day after run_date (start of the next day), so an order placed late on
    run_date has recency 0."""
    from .anomaly import daily_revenue, zscore_flags
    from .rfm import rfm_table, segment_summary
    from .serving import read_table, replace_table, sales_orders
    _, an = _engines(get_settings())
    fo = read_table(an, "fact_orders")
    sales = sales_orders(fo)
    as_of = run_date + timedelta(days=1)
    rfm = rfm_table(sales[["customer_unique_id", "order_id", "purchase_ts", "order_value"]], as_of)
    flags = zscore_flags(daily_revenue(sales, end=run_date.isoformat()))
    replace_table(an, "customer_rfm", rfm)
    replace_table(an, "daily_revenue_anomaly", flags)
    summary = segment_summary(rfm)
    print(summary[["customers", "share_customers", "share_revenue"]].to_string())
    anomalies = flags[flags["is_anomaly"]]
    print(f"[anomaly] {len(anomalies)} anomalous days:",
          ", ".join(f"{d.date()} ({z:+.1f})" for d, z in zip(anomalies["day"], anomalies["z_score"])))
    return {"customers": len(rfm), "anomalous_days": int(len(anomalies))}


def step_cosmos(run_date: date) -> dict:
    from .customer360 import build_customer_docs, build_review_docs, sync_docs
    from .serving import read_table
    s = get_settings()
    eng, an = _engines(s)
    fo = read_table(an, "fact_orders")
    fo["is_late"] = fo["is_late"].astype(int)
    docs = build_customer_docs(fo, read_table(an, "dim_customer"), read_table(an, "customer_rfm"),
                               read_table(an, "fact_reviews"), run_date)
    rv = read_table(an, "fact_reviews")
    store = cosmos_store(s)
    r1 = sync_docs(store, eng, "customer360", docs)
    r2 = sync_docs(store, eng, "reviews", build_review_docs(rv))
    return {"customer360": r1, "reviews": r2}


def step_export_powerbi(out: Path | None = None, previews: bool = True) -> dict:
    from .powerbi import EXPORT_TABLES, export, render_previews
    from .serving import read_table
    _, an = _engines(get_settings())
    tables = {t: read_table(an, t) for t in EXPORT_TABLES}
    out = Path(out or PROJECT_DIR / "powerbi")
    counts = export(tables, out / "gold_exports")
    if previews:
        for p in render_previews(tables, out / "previews"):
            print(f"[powerbi] preview {p.name}")
    return counts
