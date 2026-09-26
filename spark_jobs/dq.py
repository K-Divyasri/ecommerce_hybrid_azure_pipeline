"""
Data quality gate between silver and gold.

Each rule is a boolean Spark expression that is TRUE for a good row. A row that fails
any rule is not dropped; it is moved to a quarantine table together with the names of
the rules it broke, so someone can look at it, fix the source, and replay.

Then the gate decides: if any table's bad-row share is above the threshold (default 2%),
something upstream is broken (a bad deploy, a schema change) and building gold on top of
it would publish wrong numbers. The job raises DataQualityError and exits with code 2;
Airflow marks the task failed and nothing downstream runs.
"""
from __future__ import annotations

from dataclasses import dataclass
from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

BR_STATES = [
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA",
    "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO",
]
ORDER_STATUSES = ["created", "approved", "invoiced", "processing", "shipped", "delivered",
                  "canceled", "unavailable"]


class DataQualityError(RuntimeError):
    pass


def rules() -> dict[str, list[tuple[str, Column]]]:
    c = F.col
    return {
        "olist_orders": [
            ("order_id_not_null", c("order_id").isNotNull()),
            ("customer_id_not_null", c("customer_id").isNotNull()),
            ("status_valid", c("order_status").isin(*ORDER_STATUSES)),
            ("purchase_ts_not_null", c("order_purchase_timestamp").isNotNull()),
            ("delivered_after_purchase", c("order_delivered_customer_date").isNull()
             | (c("order_delivered_customer_date") >= c("order_purchase_timestamp"))),
        ],
        "olist_order_items": [
            ("price_positive", c("price") > 0),
            ("freight_not_negative", c("freight_value") >= 0),
            ("product_id_not_null", c("product_id").isNotNull()),
            ("seller_id_not_null", c("seller_id").isNotNull()),
        ],
        "olist_order_payments": [
            ("payment_value_not_negative", c("payment_value") >= 0),
            ("installments_not_negative", c("payment_installments") >= 0),
        ],
        "olist_order_reviews": [
            ("score_1_to_5", c("review_score").between(1, 5)),
        ],
        "olist_customers": [
            ("unique_id_not_null", c("customer_unique_id").isNotNull()),
            ("state_is_brazilian_uf", c("customer_state").isin(*BR_STATES)),
        ],
        "olist_sellers": [
            ("state_is_brazilian_uf", c("seller_state").isin(*BR_STATES)),
        ],
        "olist_products": [("product_id_not_null", c("product_id").isNotNull())],
        "product_category_name_translation": [
            ("english_not_null", c("product_category_name_english").isNotNull())],
    }


def split(df: DataFrame, table_rules: list[tuple[str, Column]]) -> tuple[DataFrame, DataFrame]:
    """Returns (good, bad). A rule that evaluates to NULL counts as a failure: "we
    could not prove this row is fine" is not the same as "fine"."""
    flags = [F.when(F.coalesce(cond, F.lit(False)), None).otherwise(F.lit(name))
             for name, cond in table_rules]
    tagged = df.withColumn("dq_failed_rules", F.concat_ws(",", *flags))
    good = tagged.filter(F.col("dq_failed_rules") == "").drop("dq_failed_rules")
    bad = tagged.filter(F.col("dq_failed_rules") != "")
    return good, bad


@dataclass
class TableResult:
    table: str
    rows: int
    bad: int

    @property
    def bad_ratio(self) -> float:
        return self.bad / self.rows if self.rows else 0.0


def run_checks(silver: dict[str, DataFrame]) -> tuple[dict[str, DataFrame], dict[str, DataFrame],
                                                     list[TableResult]]:
    """Apply row rules to every table, then the referential rule: an order item must
    point at an order that exists and passed its own checks."""
    all_rules = rules()
    good, bad, results = {}, {}, []
    for table, df in silver.items():
        df = df.cache()
        g, b = split(df, all_rules.get(table, []))
        good[table], bad[table] = g, b

    orders_ok = good["olist_orders"].select("order_id")
    items = good["olist_order_items"]
    orphans = (items.join(orders_ok, "order_id", "left_anti")
               .withColumn("dq_failed_rules", F.lit("order_exists")))
    good["olist_order_items"] = items.join(orders_ok, "order_id", "left_semi")
    bad["olist_order_items"] = bad["olist_order_items"].unionByName(orphans)

    for table, df in silver.items():
        results.append(TableResult(table, df.count(), bad[table].count()))
    return good, bad, results


def gate(results: list[TableResult], max_bad_ratio: float, log=print) -> None:
    failed = []
    for r in results:
        verdict = "FAIL" if r.bad_ratio > max_bad_ratio else "ok"
        log(f"[dq] {r.table:36s} rows={r.rows:7d} quarantined={r.bad:5d} "
            f"({r.bad_ratio:6.2%}) {verdict}")
        if verdict == "FAIL":
            failed.append(r)
    if failed:
        names = ", ".join(f"{r.table} {r.bad_ratio:.2%}" for r in failed)
        raise DataQualityError(f"bad-row share above {max_bad_ratio:.2%}: {names}. "
                               "Gold NOT written. Inspect the quarantine tables.")


def quarantine_report(bad: dict[str, DataFrame]) -> DataFrame:
    """Rule-level counts across all tables, for logs and ad hoc inspection."""
    parts = [df.select(F.lit(t).alias("table"),
                       F.explode(F.split("dq_failed_rules", ",")).alias("rule"))
             for t, df in bad.items()]
    return reduce(DataFrame.unionByName, parts).groupBy("table", "rule").count().orderBy("table", "rule")
