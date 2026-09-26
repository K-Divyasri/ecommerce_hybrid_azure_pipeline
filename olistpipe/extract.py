"""
Incremental extract: MySQL -> Parquet -> HDFS raw zone (+ copy to Azure Blob landing).

The watermark is the pair (updated_at, row_id). Using updated_at alone breaks when two
rows share the same timestamp and a page boundary falls between them; row_id breaks
the tie so every row sorts to exactly one place.

Checkpoints live in the etl_watermark table, one row per (table, run_date), holding the
window that run used: low (exclusive) and high (inclusive).

  * first run of a date     -> low = high of the latest earlier run (or the beginning of time)
  * rerun of the same date  -> reuse THAT date's recorded window, rewrite the same file
                               (overwrite, never append) -> no duplicates
  * checkpoint is written LAST, after HDFS and Blob both succeeded. A crash in the
    middle means the next attempt repeats the window, which is safe because the write
    is an overwrite. That is "at-least-once delivery + idempotent writes".
"""
from __future__ import annotations

import io
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy.engine import Engine

from .db import TS_FMT, execute, fmt_ts, query_df
from .schema import TABLES, arrow_schema

BEGINNING = "1970-01-01 00:00:00.000000"


@dataclass
class Window:
    table: str
    run_date: str
    low_ts: str
    low_row_id: int
    high_ts: str
    high_row_id: int
    row_count: int
    rerun: bool = False


class CheckpointStore:
    def __init__(self, engine: Engine):
        self.engine = engine

    def get(self, table: str, run_date: str) -> dict | None:
        df = query_df(self.engine, "SELECT * FROM etl_watermark WHERE table_name=:t AND run_date=:d",
                      {"t": table, "d": run_date})
        return None if df.empty else df.iloc[0].to_dict()

    def latest_before(self, table: str, run_date: str) -> dict | None:
        df = query_df(self.engine, """SELECT * FROM etl_watermark
            WHERE table_name=:t AND run_date < :d ORDER BY run_date DESC LIMIT 1""",
                      {"t": table, "d": run_date})
        return None if df.empty else df.iloc[0].to_dict()

    def save(self, w: Window) -> None:
        execute(self.engine, "DELETE FROM etl_watermark WHERE table_name=:t AND run_date=:d",
                {"t": w.table, "d": w.run_date})
        execute(self.engine, """INSERT INTO etl_watermark
            (table_name, run_date, low_ts, low_row_id, high_ts, high_row_id, row_count)
            VALUES (:t, :d, :lt, :li, :ht, :hi, :n)""",
                {"t": w.table, "d": w.run_date, "lt": w.low_ts, "li": int(w.low_row_id),
                 "ht": w.high_ts, "hi": int(w.high_row_id), "n": int(w.row_count)})

    def all(self) -> pd.DataFrame:
        return query_df(self.engine, "SELECT * FROM etl_watermark ORDER BY table_name, run_date")


def extract_table(engine: Engine, ckpt: CheckpointStore, table: str, run_date: date,
                  page_size: int = 20_000) -> tuple[pd.DataFrame, Window]:
    """Pull every row of `table` that changed inside this run's window.
    Keyset pagination: each page starts after the last (updated_at, row_id) seen, so
    a 10-million-row backfill never needs OFFSET (which rescans skipped rows)."""
    t = TABLES[table]
    rd = run_date.isoformat()
    cutoff = (datetime(run_date.year, run_date.month, run_date.day) + timedelta(days=1)).strftime(TS_FMT)
    existing = ckpt.get(table, rd)
    if existing:
        low_ts, low_id = existing["low_ts"], int(existing["low_row_id"])
        cap_ts, cap_id, rerun = existing["high_ts"], int(existing["high_row_id"]), True
    else:
        prev = ckpt.latest_before(table, rd)
        low_ts, low_id = (prev["high_ts"], int(prev["high_row_id"])) if prev else (BEGINNING, 0)
        cap_ts, cap_id, rerun = None, None, False

    cols = ", ".join(["row_id"] + t.column_names + ["updated_at"])
    cap_sql = ("AND (updated_at < :cap_ts OR (updated_at = :cap_ts AND row_id <= :cap_id))"
               if rerun else "")
    sql = f"""SELECT {cols} FROM {table}
        WHERE (updated_at > :lts OR (updated_at = :lts AND row_id > :lid))
          AND updated_at < :cutoff {cap_sql}
        ORDER BY updated_at, row_id
        LIMIT {int(page_size)}"""

    pages, page_low_ts, page_low_id = [], low_ts, low_id
    while True:
        params = {"lts": page_low_ts, "lid": page_low_id, "cutoff": cutoff}
        if rerun:
            params.update(cap_ts=cap_ts, cap_id=cap_id)
        page = query_df(engine, sql, params)
        if page.empty:
            break
        pages.append(page)
        last = page.iloc[-1]
        page_low_ts, page_low_id = fmt_ts(last["updated_at"]), int(last["row_id"])
        if len(page) < page_size:
            break

    df = pd.concat(pages, ignore_index=True) if pages else pd.DataFrame(
        columns=["row_id"] + t.column_names + ["updated_at"])
    if rerun:
        high_ts, high_id = cap_ts, cap_id
    elif len(df):
        high_ts, high_id = page_low_ts, page_low_id
    else:
        high_ts, high_id = low_ts, low_id
    return df, Window(table, rd, low_ts, low_id, high_ts, high_id, len(df), rerun)


def to_parquet_bytes(df: pd.DataFrame, table: str) -> bytes:
    """Cast to the table's fixed Arrow schema and serialise. Fixed types matter: a day
    where every product_name_lenght is null must still write int64, not null-type,
    or Spark refuses to read the partitions together."""
    schema = arrow_schema(table)
    t = TABLES[table]
    out = pd.DataFrame(index=df.index)
    for field in schema:
        col = field.name
        ty = t.col_type(col)
        s = df[col] if col in df else pd.Series([None] * len(df), index=df.index)
        if ty == "ts":
            s = pd.to_datetime(s, errors="coerce")
            s = s.dt.tz_localize("UTC") if s.dt.tz is None else s
        elif ty == "int":
            s = pd.to_numeric(s, errors="coerce").astype("Int64")
        elif ty == "float":
            s = pd.to_numeric(s.astype(object).where(s.notna(), None), errors="coerce").astype("float64")
        else:
            s = s.astype(object).where(s.notna(), None)
        out[col] = s
    table_pa = pa.Table.from_pandas(out, schema=schema, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table_pa, buf, compression="snappy", coerce_timestamps="us")
    return buf.getvalue()


def raw_path(root: str, table: str, run_date: str) -> str:
    return f"{root.rstrip('/')}/{table}/ingest_date={run_date}/part-00000.parquet"


def run_extract(engine: Engine, ckpt: CheckpointStore, raw_store, blob_store, raw_root: str,
                run_date: date, tables: list[str] | None = None, log=print) -> list[Window]:
    """Extract every table for one run date. Order: HDFS write, Blob copy, checkpoint."""
    windows = []
    for table in tables or list(TABLES):
        df, w = extract_table(engine, ckpt, table, run_date)
        data = to_parquet_bytes(df, table)
        path = raw_path(raw_root, table, w.run_date)
        raw_store.put_bytes(path, data)
        if blob_store is not None:
            blob_store.put_bytes("olist" + path.replace(raw_root.rstrip("/"), "/raw", 1), data)
        ckpt.save(w)
        tag = "RERUN same window" if w.rerun else "new window"
        log(f"[extract] {table:36s} rows={w.row_count:6d} ({tag}) -> {path}")
        windows.append(w)
    return windows
