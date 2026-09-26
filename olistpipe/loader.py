"""
Seed the OLTP database from CSVs (generated or the real Kaggle files).

The Kaggle CSVs have no updated_at column, so the loader derives one per row from the
business timestamps: "the last moment this row could have changed". That turns a static
dataset into something that looks like it came out of a live application, which is what
the incremental extract needs.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from .db import TS_FMT, is_sqlite
from .schema import TABLES

BASELINE = pd.Timestamp("2016-09-01 00:00:00")


def derive_updated_at(frames: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    out = {k: v.copy() for k, v in frames.items()}
    orders = out["olist_orders"]
    ts_cols = ["order_purchase_timestamp", "order_approved_at",
               "order_delivered_carrier_date", "order_delivered_customer_date"]
    as_dt = orders[ts_cols].apply(pd.to_datetime, errors="coerce")
    orders["updated_at"] = as_dt.max(axis=1)
    purchase = pd.to_datetime(orders.set_index("order_id")["order_purchase_timestamp"])
    by_customer = pd.to_datetime(orders.set_index("customer_id")["order_purchase_timestamp"])

    for name in ("olist_order_items", "olist_order_payments"):
        df = out[name]
        df["updated_at"] = df["order_id"].map(purchase)

    rev = out["olist_order_reviews"]
    rev["updated_at"] = pd.to_datetime(rev["review_answer_timestamp"]).fillna(
        pd.to_datetime(rev["review_creation_date"]))

    cust = out["olist_customers"]
    cust["updated_at"] = cust["customer_id"].map(by_customer)

    items = out["olist_order_items"]
    first_use = items.groupby("product_id")["updated_at"].min()
    out["olist_products"]["updated_at"] = out["olist_products"]["product_id"].map(first_use)
    first_sale = items.groupby("seller_id")["updated_at"].min()
    out["olist_sellers"]["updated_at"] = out["olist_sellers"]["seller_id"].map(first_sale)
    out["product_category_name_translation"]["updated_at"] = BASELINE

    for name, df in out.items():
        df["updated_at"] = pd.to_datetime(df["updated_at"]).fillna(BASELINE)
    return out


def _records(df: pd.DataFrame, table: str) -> list[dict]:
    t = TABLES[table]
    df = df.copy()
    for col, ty in t.columns:
        if ty == "ts":
            s = pd.to_datetime(df[col], errors="coerce")
            df[col] = s.dt.strftime("%Y-%m-%d %H:%M:%S").where(s.notna(), None)
    df["updated_at"] = pd.to_datetime(df["updated_at"]).dt.strftime(TS_FMT)
    df = df.astype(object).where(pd.notna(df), None)
    recs = df.to_dict("records")
    for r in recs:  # numpy scalars -> python, pymysql does not know numpy types
        for k, v in r.items():
            if isinstance(v, np.generic):
                r[k] = v.item()
    return recs


def seed(engine: Engine, frames: dict[str, pd.DataFrame], chunk: int = 5000) -> dict[str, int]:
    """Truncate and reload every Olist table. Safe to rerun (full replace)."""
    frames = derive_updated_at(frames)
    counts = {}
    with engine.begin() as conn:
        for name in TABLES:
            if is_sqlite(engine):
                conn.execute(text(f"DELETE FROM {name}"))
            else:
                conn.execute(text(f"TRUNCATE TABLE {name}"))
        conn.execute(text("DELETE FROM etl_watermark"))
    for name, t in TABLES.items():
        cols = t.column_names + ["updated_at"]
        sql = (f"INSERT INTO {name} ({', '.join(cols)}) "
               f"VALUES ({', '.join(':' + c for c in cols)})")
        recs = _records(frames[name][cols], name)
        with engine.begin() as conn:
            for i in range(0, len(recs), chunk):
                conn.execute(text(sql), recs[i:i + chunk])
        counts[name] = len(recs)
    return counts
