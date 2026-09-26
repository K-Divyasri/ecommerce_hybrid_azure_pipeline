"""
The eight Olist tables, described once and used everywhere.

Column names match the Kaggle "Brazilian E-Commerce Public Dataset by Olist" exactly,
including the dataset's own spelling mistakes (product_name_lenght). Keeping the real
names means the real CSVs drop straight in with no renaming.

Every table in MySQL also gets two technical columns the Kaggle CSVs do not have:

    row_id      BIGINT AUTO_INCREMENT  -> surrogate id, used as the watermark tie-break
    updated_at  DATETIME(6)            -> last time the row changed, used as the watermark

Types are kept deliberately small: str, int, float, ts (timestamp).
"""
from __future__ import annotations

from dataclasses import dataclass, field

import pyarrow as pa


@dataclass(frozen=True)
class Table:
    name: str                      # MySQL table name
    csv: str                       # Kaggle file name
    columns: list[tuple[str, str]]  # (column, type) in Kaggle order
    key: list[str]                 # natural key (unique in MySQL)
    sizes: dict[str, int] = field(default_factory=dict)  # VARCHAR sizes, default 64

    @property
    def column_names(self) -> list[str]:
        return [c for c, _ in self.columns]

    def col_type(self, col: str) -> str:
        if col == "updated_at":
            return "ts"
        if col == "row_id":
            return "int"
        return dict(self.columns)[col]


TABLES: dict[str, Table] = {}


def _add(t: Table) -> None:
    TABLES[t.name] = t


_add(Table(
    "olist_customers", "olist_customers_dataset.csv",
    [("customer_id", "str"), ("customer_unique_id", "str"), ("customer_zip_code_prefix", "str"),
     ("customer_city", "str"), ("customer_state", "str")],
    ["customer_id"],
))
_add(Table(
    "olist_orders", "olist_orders_dataset.csv",
    [("order_id", "str"), ("customer_id", "str"), ("order_status", "str"),
     ("order_purchase_timestamp", "ts"), ("order_approved_at", "ts"),
     ("order_delivered_carrier_date", "ts"), ("order_delivered_customer_date", "ts"),
     ("order_estimated_delivery_date", "ts")],
    ["order_id"],
))
_add(Table(
    "olist_order_items", "olist_order_items_dataset.csv",
    [("order_id", "str"), ("order_item_id", "int"), ("product_id", "str"), ("seller_id", "str"),
     ("shipping_limit_date", "ts"), ("price", "float"), ("freight_value", "float")],
    ["order_id", "order_item_id"],
))
_add(Table(
    "olist_order_payments", "olist_order_payments_dataset.csv",
    [("order_id", "str"), ("payment_sequential", "int"), ("payment_type", "str"),
     ("payment_installments", "int"), ("payment_value", "float")],
    ["order_id", "payment_sequential"],
))
_add(Table(
    "olist_order_reviews", "olist_order_reviews_dataset.csv",
    [("review_id", "str"), ("order_id", "str"), ("review_score", "int"),
     ("review_comment_title", "str"), ("review_comment_message", "str"),
     ("review_creation_date", "ts"), ("review_answer_timestamp", "ts")],
    # review_id alone is NOT unique in the real dataset (a few reviews cover two orders)
    ["review_id", "order_id"],
    {"review_comment_title": 128, "review_comment_message": 1000},
))
_add(Table(
    "olist_products", "olist_products_dataset.csv",
    [("product_id", "str"), ("product_category_name", "str"), ("product_name_lenght", "int"),
     ("product_description_lenght", "int"), ("product_photos_qty", "int"),
     ("product_weight_g", "int"), ("product_length_cm", "int"), ("product_height_cm", "int"),
     ("product_width_cm", "int")],
    ["product_id"],
))
_add(Table(
    "olist_sellers", "olist_sellers_dataset.csv",
    [("seller_id", "str"), ("seller_zip_code_prefix", "str"), ("seller_city", "str"),
     ("seller_state", "str")],
    ["seller_id"],
))
_add(Table(
    "product_category_name_translation", "product_category_name_translation.csv",
    [("product_category_name", "str"), ("product_category_name_english", "str")],
    ["product_category_name"],
))

TABLE_NAMES = list(TABLES)

# The 27 Brazilian federative units. Anything else in a *_state column is bad data.
BR_STATES = [
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA",
    "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO",
]

ORDER_STATUSES = [
    "created", "approved", "invoiced", "processing", "shipped", "delivered",
    "canceled", "unavailable",
]


def arrow_schema(table: str) -> pa.Schema:
    """Fixed Parquet schema per table. Every daily file gets the SAME types, even on days
    when a column happens to be all null, so Spark can read all partitions together."""
    t = TABLES[table]
    mapping = {
        "str": pa.string(),
        "int": pa.int64(),
        "float": pa.float64(),
        # microseconds + UTC: Spark 3.5 cannot read nanosecond Parquet timestamps
        "ts": pa.timestamp("us", tz="UTC"),
    }
    fields = [pa.field("row_id", pa.int64())]
    fields += [pa.field(c, mapping[ty]) for c, ty in t.columns]
    fields += [pa.field("updated_at", mapping["ts"])]
    return pa.schema(fields)


def mysql_ddl() -> str:
    """CREATE TABLE statements for the OLTP schema. sql/mysql/02_olist_tables.sql is
    generated from this function (a test checks the file has not drifted)."""
    out = ["-- GENERATED by `python -m olistpipe ddl`. Edit olistpipe/schema.py, not this file.",
           "USE olist;", ""]
    for t in TABLES.values():
        lines = ["  row_id BIGINT NOT NULL AUTO_INCREMENT"]
        for col, ty in t.columns:
            if ty == "str":
                size = t.sizes.get(col, 64)
                sqlt = f"VARCHAR({size})" if size <= 255 else "TEXT"
            elif ty == "int":
                sqlt = "INT"
            elif ty == "float":
                sqlt = "DECIMAL(12,2)"
            else:
                sqlt = "DATETIME"
            null = " NOT NULL" if col in t.key else " NULL"
            lines.append(f"  {col} {sqlt}{null}")
        lines.append("  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) "
                     "ON UPDATE CURRENT_TIMESTAMP(6)")
        lines.append("  PRIMARY KEY (row_id)")
        lines.append(f"  UNIQUE KEY uq_{t.name} ({', '.join(t.key)})")
        lines.append(f"  KEY ix_{t.name}_watermark (updated_at, row_id)")
        out.append(f"CREATE TABLE IF NOT EXISTS {t.name} (\n" + ",\n".join(lines) + "\n);\n")
    return "\n".join(out)


def sqlite_ddl() -> list[str]:
    """Same tables for SQLite, used by the offline tests."""
    stmts = []
    for t in TABLES.values():
        cols = ["row_id INTEGER PRIMARY KEY AUTOINCREMENT"]
        for col, ty in t.columns:
            sqlt = {"str": "TEXT", "int": "INTEGER", "float": "REAL", "ts": "TIMESTAMP"}[ty]
            cols.append(f"{col} {sqlt}")
        cols.append("updated_at TIMESTAMP NOT NULL")
        cols.append(f"UNIQUE ({', '.join(t.key)})")
        stmts.append(f"CREATE TABLE IF NOT EXISTS {t.name} ({', '.join(cols)})")
    return stmts
