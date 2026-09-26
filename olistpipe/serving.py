"""
Read gold/serving tables into pandas and write small pandas results back.

Spark owns the big star-schema tables in the analytics database. pandas owns the two
small analytical outputs (customer_rfm, daily_revenue_anomaly) and writes them with a
plain drop-and-recreate: they are rebuilt in full every run, so that is idempotent.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from .db import query_df


def read_table(engine: Engine, table: str, columns: str = "*") -> pd.DataFrame:
    """Rows come back sorted by the first column. Without ORDER BY, MySQL may return rows
    in a different order each time, and summing floats in a different order can change
    the last cent after rounding: enough to make a rerun look like a change."""
    return query_df(engine, f"SELECT {columns} FROM {table} ORDER BY 1")


def _sql_type(s: pd.Series) -> str:
    if pd.api.types.is_bool_dtype(s):
        return "BOOLEAN"
    if pd.api.types.is_integer_dtype(s):
        return "BIGINT"
    if pd.api.types.is_float_dtype(s):
        return "DOUBLE"
    if pd.api.types.is_datetime64_any_dtype(s):
        return "DATETIME"
    return "VARCHAR(255)"


def replace_table(engine: Engine, table: str, df: pd.DataFrame, chunk: int = 5000) -> int:
    cols = list(df.columns)
    ddl = ", ".join(f"{c} {_sql_type(df[c])}" for c in cols)
    clean = df.copy()
    for c in cols:
        if pd.api.types.is_datetime64_any_dtype(clean[c]):
            clean[c] = clean[c].dt.strftime("%Y-%m-%d %H:%M:%S")
    clean = clean.astype(object).where(pd.notna(clean), None)
    recs = clean.to_dict("records")
    for r in recs:
        for k, v in r.items():
            if isinstance(v, np.generic):
                r[k] = v.item()
    ins = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join(':' + c for c in cols)})"
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {table}"))
        conn.execute(text(f"CREATE TABLE {table} ({ddl})"))
        for i in range(0, len(recs), chunk):
            conn.execute(text(ins), recs[i:i + chunk])
    return len(recs)


def sales_orders(fact_orders: pd.DataFrame) -> pd.DataFrame:
    """Orders that count as revenue. MySQL returns BOOLEAN as 0/1, so compare == 1."""
    fo = fact_orders.copy()
    fo["order_value"] = fo["order_value"].astype(float)
    return fo[fo["is_sale"].astype(int) == 1]
