import io
from datetime import date

import pyarrow.parquet as pq

from olistpipe.db import execute, query_df
from olistpipe.extract import CheckpointStore, extract_table, raw_path, run_extract
from olistpipe.simulator import simulate_day
from olistpipe.storage import LocalStore

QUIET = dict(log=lambda *a: None)


def _read(store, table, day):
    return pq.read_table(io.BytesIO(store.get_bytes(raw_path("/olist/raw", table, day)))).to_pandas()


def test_first_run_takes_everything_before_cutoff(seeded_engine, tmp_path):
    ck = CheckpointStore(seeded_engine)
    store = LocalStore(tmp_path)
    run_extract(seeded_engine, ck, store, None, "/olist/raw", date(2018, 8, 31), **QUIET)
    total = query_df(seeded_engine, "SELECT COUNT(*) n FROM olist_orders "
                     "WHERE updated_at < '2018-09-01 00:00:00.000000'")["n"][0]
    assert len(_read(store, "olist_orders", "2018-08-31")) == total


def test_rerun_same_day_is_identical(seeded_engine, tmp_path):
    ck = CheckpointStore(seeded_engine)
    store = LocalStore(tmp_path)
    run_extract(seeded_engine, ck, store, None, "/olist/raw", date(2018, 8, 31), **QUIET)
    simulate_day(seeded_engine, date(2018, 9, 1))
    run_extract(seeded_engine, ck, store, None, "/olist/raw", date(2018, 9, 1), **QUIET)
    first = _read(store, "olist_orders", "2018-09-01")
    windows = run_extract(seeded_engine, ck, store, None, "/olist/raw", date(2018, 9, 1), **QUIET)
    second = _read(store, "olist_orders", "2018-09-01")
    assert all(w.rerun for w in windows)
    assert first.equals(second)
    assert first["order_id"].is_unique
    assert len(first) > 0


def test_second_day_only_gets_changes(seeded_engine, tmp_path):
    ck = CheckpointStore(seeded_engine)
    store = LocalStore(tmp_path)
    run_extract(seeded_engine, ck, store, None, "/olist/raw", date(2018, 8, 31), **QUIET)
    stats = simulate_day(seeded_engine, date(2018, 9, 1), new_orders=25)
    run_extract(seeded_engine, ck, store, None, "/olist/raw", date(2018, 9, 1), **QUIET)
    day2 = _read(store, "olist_customers", "2018-09-01")
    assert len(day2) == stats["new_orders"] == 25
    items_day2 = _read(store, "olist_products", "2018-09-01")
    assert len(items_day2) == 0  # products did not change; empty file with the fixed schema
    assert "product_weight_g" in items_day2.columns


def test_tie_break_on_row_id_with_tiny_pages(seeded_engine):
    """Force 30 rows onto the SAME updated_at and page through them 7 at a time.
    Without the row_id tie-break a page boundary inside the tie would lose or repeat rows."""
    execute(seeded_engine, "UPDATE olist_sellers SET updated_at='2018-08-01 12:00:00.000000'")
    ck = CheckpointStore(seeded_engine)
    df, w = extract_table(seeded_engine, ck, "olist_sellers", date(2018, 8, 31), page_size=7)
    n = query_df(seeded_engine, "SELECT COUNT(*) n FROM olist_sellers")["n"][0]
    assert len(df) == n
    assert df["seller_id"].is_unique


def test_checkpoint_advances(seeded_engine, tmp_path):
    ck = CheckpointStore(seeded_engine)
    store = LocalStore(tmp_path)
    run_extract(seeded_engine, ck, store, None, "/olist/raw", date(2018, 8, 31), **QUIET)
    simulate_day(seeded_engine, date(2018, 9, 1))
    run_extract(seeded_engine, ck, store, None, "/olist/raw", date(2018, 9, 1), **QUIET)
    rows = ck.all()
    o = rows[rows.table_name == "olist_orders"].set_index("run_date")
    assert o.loc["2018-09-01", "low_ts"] == o.loc["2018-08-31", "high_ts"]
    assert o.loc["2018-09-01", "high_ts"] > o.loc["2018-09-01", "low_ts"]


def test_blob_copy_written(seeded_engine, tmp_path):
    ck = CheckpointStore(seeded_engine)
    raw, blob = LocalStore(tmp_path / "hdfs"), LocalStore(tmp_path / "blob")
    run_extract(seeded_engine, ck, raw, blob, "/olist/raw", date(2018, 8, 31), **QUIET)
    same = raw.get_bytes(raw_path("/olist/raw", "olist_orders", "2018-08-31"))
    assert blob.get_bytes("olist/raw/olist_orders/ingest_date=2018-08-31/part-00000.parquet") == same


def test_simulator_deterministic_and_refuses_replay(small_frames):
    from olistpipe.db import sqlite_engine
    from olistpipe.loader import seed
    results = []
    for _ in range(2):
        eng = sqlite_engine()
        seed(eng, small_frames)
        results.append(simulate_day(eng, date(2018, 9, 1)))
    assert results[0] == results[1]
    assert "skipped_day_already_has_orders" in simulate_day(eng, date(2018, 9, 1))
