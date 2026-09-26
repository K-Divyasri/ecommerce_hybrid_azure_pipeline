"""
Database helpers that work the same on MySQL (real pipeline) and SQLite (tests).

Everything goes through SQLAlchemy Core `text()` with :named parameters, and results
are turned into DataFrames by hand. That avoids pandas.read_sql version quirks and runs
on both SQLAlchemy 1.4 (inside the Airflow image) and 2.x (host).

Timestamps that take part in watermark comparisons are always written as
'YYYY-MM-DD HH:MM:SS.ffffff'. MySQL compares them as DATETIME; SQLite compares them as
text, which only works if every value has the same shape.
"""
from __future__ import annotations

from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from .schema import sqlite_ddl

TS_FMT = "%Y-%m-%d %H:%M:%S.%f"

ETL_DDL = [
    """CREATE TABLE IF NOT EXISTS etl_watermark (
        table_name VARCHAR(64) NOT NULL,
        run_date VARCHAR(10) NOT NULL,
        low_ts VARCHAR(26) NOT NULL,
        low_row_id BIGINT NOT NULL,
        high_ts VARCHAR(26) NOT NULL,
        high_row_id BIGINT NOT NULL,
        row_count INT NOT NULL,
        PRIMARY KEY (table_name, run_date)
    )""",
    """CREATE TABLE IF NOT EXISTS etl_cosmos_sync (
        doc_id VARCHAR(64) NOT NULL,
        container VARCHAR(32) NOT NULL,
        doc_hash VARCHAR(64) NOT NULL,
        PRIMARY KEY (container, doc_id)
    )""",
]


def fmt_ts(value) -> str:
    if isinstance(value, str):
        value = pd.Timestamp(value)
    if isinstance(value, pd.Timestamp):
        value = value.to_pydatetime()
    if getattr(value, "tzinfo", None) is not None:
        value = value.replace(tzinfo=None)
    return value.strftime(TS_FMT)


def mysql_engine(url: str) -> Engine:
    return create_engine(url, pool_pre_ping=True, future=True)


def sqlite_engine(path: str = ":memory:") -> Engine:
    """In-memory SQLite with the Olist tables and the ETL control tables. The
    StaticPool keeps one connection so :memory: data survives between calls."""
    from sqlalchemy.pool import StaticPool
    eng = create_engine(f"sqlite:///{path}", future=True, poolclass=StaticPool,
                        connect_args={"check_same_thread": False})
    with eng.begin() as conn:
        for stmt in sqlite_ddl() + ETL_DDL:
            conn.execute(text(stmt))
    return eng


def query_df(engine: Engine, sql: str, params: dict | None = None) -> pd.DataFrame:
    with engine.connect() as conn:
        res = conn.execute(text(sql), params or {})
        rows = res.fetchall()
        return pd.DataFrame([tuple(r) for r in rows], columns=list(res.keys()))


def execute(engine: Engine, sql: str, params=None) -> int:
    """Run one statement (params may be a dict or a list of dicts for executemany)."""
    with engine.begin() as conn:
        res = conn.execute(text(sql), params or {})
        return res.rowcount


def is_sqlite(engine: Engine) -> bool:
    return engine.dialect.name == "sqlite"


def now_str(dt: datetime) -> str:
    return dt.strftime(TS_FMT)
