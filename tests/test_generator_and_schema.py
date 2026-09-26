import pandas as pd
import pyarrow as pa

from conftest import PROJECT, SAMPLE
from olistpipe.generator import generate, read_csvs
from olistpipe.schema import TABLES, arrow_schema, mysql_ddl


def test_generator_is_deterministic():
    a = generate(n_orders=200, seed=5)
    b = generate(n_orders=200, seed=5)
    for name in a:
        pd.testing.assert_frame_equal(a[name], b[name])
    c = generate(n_orders=200, seed=6)
    assert not a["olist_orders"]["order_id"].equals(c["olist_orders"]["order_id"])


def test_columns_match_kaggle_names(small_frames):
    for name, t in TABLES.items():
        assert list(small_frames[name].columns) == t.column_names
    # the dataset's own typo is preserved on purpose
    assert "product_name_lenght" in small_frames["olist_products"].columns


def test_olist_quirks_present(small_frames):
    cust = small_frames["olist_customers"]
    # customer_id is per order, customer_unique_id is the person
    assert cust["customer_id"].is_unique
    assert cust["customer_unique_id"].nunique() < len(cust)
    # a category with no English translation, like pc_gamer in real Olist
    prods = small_frames["olist_products"]["product_category_name"].dropna().unique()
    translated = set(small_frames["product_category_name_translation"]["product_category_name"])
    assert "pc_gamer" not in translated
    # Sao Paulo is the biggest state by far
    states = cust["customer_state"].str.strip().str.upper().value_counts()
    assert states.index[0] == "SP"
    assert len(prods) > 5


def test_dirty_rows_injected(small_frames):
    items = small_frames["olist_order_items"]
    reviews = small_frames["olist_order_reviews"]
    assert (items["price"] <= 0).sum() >= 1 or (reviews["review_score"] > 5).sum() >= 1
    clean = generate(n_orders=400, seed=11, dirty_rate=0)
    assert (clean["olist_order_items"]["price"] > 0).all()
    assert clean["olist_order_reviews"]["review_score"].between(1, 5).all()


def test_committed_sample_reads_back():
    frames = read_csvs(SAMPLE)
    assert len(frames["olist_orders"]) == 1000
    # zip codes stay strings (leading zeros survive)
    assert frames["olist_customers"]["customer_zip_code_prefix"].str.len().eq(5).all()


def test_ddl_file_not_drifted():
    on_disk = (PROJECT / "sql" / "mysql" / "02_olist_tables.sql").read_text()
    assert on_disk.strip() == mysql_ddl().strip()


def test_arrow_schema_uses_microsecond_utc():
    s = arrow_schema("olist_orders")
    assert s.field("order_purchase_timestamp").type == pa.timestamp("us", tz="UTC")
    assert s.names[0] == "row_id" and s.names[-1] == "updated_at"
