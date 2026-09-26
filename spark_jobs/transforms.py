"""
Pure Spark transforms: DataFrames in, DataFrames out, no I/O.

Keeping I/O out of this file is what makes it testable: tests build tiny DataFrames by
hand, call these functions, and check the answer. build_gold.py does the reading and
writing around them.

Layers:
    raw     every version of every row, one Parquet file per table per ingest day
    silver  latest version per key (row_number dedupe), cleaned, quality-checked
    gold    star schema (facts + dimensions) and aggregates for BI
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

NATURAL_KEYS = {
    "olist_customers": ["customer_id"],
    "olist_orders": ["order_id"],
    "olist_order_items": ["order_id", "order_item_id"],
    "olist_order_payments": ["order_id", "payment_sequential"],
    "olist_order_reviews": ["review_id", "order_id"],
    "olist_products": ["product_id"],
    "olist_sellers": ["seller_id"],
    "product_category_name_translation": ["product_category_name"],
}
SALES_EXCLUDED = ["canceled", "unavailable"]


# --------------------------------------------------------------------------- silver
def latest_version(df: DataFrame, key: list[str]) -> DataFrame:
    """Keep one row per natural key: the newest. The raw zone holds every version ever
    extracted (an order appears once when created, again when shipped, again when
    delivered). row_number over (key ORDER BY updated_at DESC, row_id DESC) numbers
    each key's versions newest-first; keeping number 1 keeps the latest."""
    order = [F.col("updated_at").desc(), F.col("row_id").desc()]
    if "ingest_date" in df.columns:
        order.append(F.col("ingest_date").desc())
    w = Window.partitionBy(*key).orderBy(*order)
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def _clean_place(df: DataFrame, prefix: str) -> DataFrame:
    return (df.withColumn(f"{prefix}_city", F.lower(F.trim(F.col(f"{prefix}_city"))))
              .withColumn(f"{prefix}_state", F.upper(F.trim(F.col(f"{prefix}_state"))))
              .withColumn(f"{prefix}_zip_code_prefix",
                          F.lpad(F.trim(F.col(f"{prefix}_zip_code_prefix")), 5, "0")))


def _blank_to_null(c: str):
    return F.when(F.length(F.trim(F.col(c))) == 0, None).otherwise(F.trim(F.col(c)))


def clean(table: str, df: DataFrame) -> DataFrame:
    """Fix what can be fixed (case, whitespace, padding). Do NOT fix what cannot be
    fixed safely (a negative price): that is the quality gate's job."""
    if table == "olist_customers":
        df = _clean_place(df, "customer")
    elif table == "olist_sellers":
        df = _clean_place(df, "seller")
    elif table == "olist_orders":
        df = df.withColumn("order_status", F.lower(F.trim("order_status")))
    elif table == "olist_products":
        df = df.withColumn("product_category_name", _blank_to_null("product_category_name"))
    elif table == "olist_order_reviews":
        df = (df.withColumn("review_comment_title", _blank_to_null("review_comment_title"))
                .withColumn("review_comment_message", _blank_to_null("review_comment_message")))
    elif table == "olist_order_payments":
        df = df.withColumn("payment_type", F.lower(F.trim("payment_type")))
    return df


def to_silver(raw: dict[str, DataFrame]) -> dict[str, DataFrame]:
    return {t: clean(t, latest_version(df, NATURAL_KEYS[t])) for t, df in raw.items()}


# --------------------------------------------------------------------------- gold: dims
def date_key(col):
    return F.date_format(col, "yyyyMMdd").cast("int")


def build_dim_date(spark: SparkSession, start: str, end: str) -> DataFrame:
    df = spark.sql(f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), "
                   f"interval 1 day)) AS date")
    return (df.select(
        date_key(F.col("date")).alias("date_key"), "date",
        F.year("date").alias("year"), F.quarter("date").alias("quarter"),
        F.month("date").alias("month"), F.date_format("date", "MMM").alias("month_name"),
        F.date_format("date", "yyyy-MM").alias("year_month"),
        F.dayofmonth("date").alias("day"),
        F.date_format("date", "E").alias("day_name"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend")))


def build_dim_customer(customers: DataFrame, orders: DataFrame) -> DataFrame:
    """Grain: ONE ROW PER PERSON (customer_unique_id). Olist's customer_id is minted per
    order, so counting customer_id counts orders, not people. Address = the one used on
    the person's most recent order."""
    co = customers.join(orders.select("customer_id", "order_purchase_timestamp"), "customer_id", "left")
    w = Window.partitionBy("customer_unique_id").orderBy(
        F.col("order_purchase_timestamp").desc_nulls_last(), F.col("customer_id"))
    latest = co.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
    firsts = co.groupBy("customer_unique_id").agg(
        F.min("order_purchase_timestamp").alias("first_order_ts"),
        F.countDistinct("customer_id").alias("order_ids_seen"))
    return (latest.select("customer_unique_id", "customer_city", "customer_state",
                          "customer_zip_code_prefix")
                  .join(firsts, "customer_unique_id"))


def build_dim_product(products: DataFrame, translation: DataFrame) -> DataFrame:
    """translation has ~70 rows: F.broadcast ships a copy to every executor so the big
    side never shuffles. Categories missing from the translation (pc_gamer in real
    Olist) fall back to the Portuguese name instead of disappearing."""
    p = products.join(F.broadcast(translation.select("product_category_name",
                                                     "product_category_name_english")),
                      "product_category_name", "left")
    return p.select(
        "product_id",
        F.coalesce("product_category_name", F.lit("unknown")).alias("category_pt"),
        F.coalesce("product_category_name_english", "product_category_name",
                   F.lit("unknown")).alias("category"),
        F.col("product_photos_qty").alias("photos_qty"),
        F.col("product_weight_g").alias("weight_g"),
        (F.col("product_length_cm") * F.col("product_height_cm") * F.col("product_width_cm"))
        .alias("volume_cm3"))


def build_dim_seller(sellers: DataFrame) -> DataFrame:
    return sellers.select("seller_id", "seller_city", "seller_state", "seller_zip_code_prefix")


# --------------------------------------------------------------------------- gold: facts
def build_fact_order_items(items, orders, customers) -> DataFrame:
    """Grain: one row per order line. The only grain where seller and product are exact
    (an order can hold items from different sellers)."""
    o = orders.select("order_id", "customer_id", "order_status", "order_purchase_timestamp",
                      "order_delivered_carrier_date")
    c = customers.select("customer_id", "customer_unique_id")
    return (items.join(o, "order_id").join(c, "customer_id", "left")
            .select("order_id", "order_item_id", "product_id", "seller_id", "customer_unique_id",
                    date_key(F.col("order_purchase_timestamp")).alias("purchase_date_key"),
                    "order_status", "price", "freight_value",
                    (F.col("price") + F.col("freight_value")).alias("item_value"),
                    F.col("shipping_limit_date").alias("shipping_limit_ts"),
                    F.col("order_delivered_carrier_date").alias("carrier_ts"),
                    (~F.col("order_status").isin(*SALES_EXCLUDED)).alias("is_sale")))


def build_fact_orders(orders, items, payments, reviews, customers) -> DataFrame:
    """Grain: one row per order. Items and payments are pre-aggregated to order level
    BEFORE joining, otherwise an order with 3 items and 2 payments would fan out to 6
    rows and every sum would be wrong (the classic join fan-out bug)."""
    it = items.groupBy("order_id").agg(
        F.count("*").alias("item_count"), F.sum("price").alias("items_value"),
        F.sum("freight_value").alias("freight_value"),
        F.countDistinct("seller_id").alias("seller_count"))
    pay = payments.groupBy("order_id").agg(
        F.sum("payment_value").alias("payment_value"),
        F.max("payment_installments").alias("max_installments"),
        F.max_by("payment_type", "payment_value").alias("main_payment_type"))
    rv = reviews.groupBy("order_id").agg(F.avg("review_score").alias("review_score"))
    c = customers.select("customer_id", "customer_unique_id")
    delivered = F.col("order_delivered_customer_date")
    purchase = F.col("order_purchase_timestamp")
    estimated = F.col("order_estimated_delivery_date")
    return (orders.join(c, "customer_id", "left").join(it, "order_id", "left")
            .join(pay, "order_id", "left").join(rv, "order_id", "left")
            .select(
                "order_id", "customer_unique_id",
                date_key(purchase).alias("purchase_date_key"),
                purchase.alias("purchase_ts"), "order_status",
                F.coalesce("item_count", F.lit(0)).alias("item_count"),
                F.coalesce("seller_count", F.lit(0)).alias("seller_count"),
                F.round(F.coalesce("items_value", F.lit(0.0)), 2).alias("items_value"),
                F.round(F.coalesce("freight_value", F.lit(0.0)), 2).alias("freight_value"),
                F.round(F.coalesce("items_value", F.lit(0.0)) + F.coalesce("freight_value", F.lit(0.0)), 2)
                .alias("order_value"),
                F.round("payment_value", 2).alias("payment_value"), "main_payment_type",
                "max_installments",
                delivered.alias("delivered_ts"), estimated.alias("estimated_ts"),
                delivered.isNotNull().alias("is_delivered"),
                F.round((F.unix_timestamp(delivered) - F.unix_timestamp(purchase)) / 86400, 2)
                .alias("delivery_days"),
                F.datediff(F.to_date(delivered), F.to_date(estimated)).alias("delay_days"),
                F.coalesce(F.to_date(delivered) > F.to_date(estimated), F.lit(False)).alias("is_late"),
                F.round("review_score", 2).alias("review_score"),
                (~F.col("order_status").isin(*SALES_EXCLUDED)).alias("is_sale")))


def build_fact_reviews(reviews, orders, customers) -> DataFrame:
    o = orders.select("order_id", "customer_id")
    c = customers.select("customer_id", "customer_unique_id")
    return (reviews.join(o, "order_id", "left").join(c, "customer_id", "left")
            .select("review_id", "order_id", "customer_unique_id", "review_score",
                    "review_comment_title", "review_comment_message",
                    F.col("review_creation_date").alias("review_creation_ts"),
                    F.col("review_answer_timestamp").alias("review_answer_ts")))


# --------------------------------------------------------------------------- aggregates
def agg_delivery_by_state(fact_orders: DataFrame, dim_customer: DataFrame) -> DataFrame:
    d = (fact_orders.filter("is_delivered")
         .join(dim_customer.select("customer_unique_id", "customer_state"), "customer_unique_id"))
    return (d.groupBy("customer_state").agg(
        F.count("*").alias("delivered_orders"),
        F.sum(F.col("is_late").cast("int")).alias("late_orders"),
        F.round(F.avg(F.col("is_late").cast("double")) * 100, 2).alias("late_pct"),
        F.round(F.avg("delivery_days"), 2).alias("avg_delivery_days"),
        F.round(F.avg(F.when(F.col("is_late"), F.col("delay_days"))), 2).alias("avg_days_late_when_late"),
        F.round(F.avg("review_score"), 2).alias("avg_review_score"))
        .orderBy(F.col("late_pct").desc()))


def agg_seller_scorecard(fact_items: DataFrame, fact_orders: DataFrame,
                         dim_seller: DataFrame, min_orders: int = 3) -> DataFrame:
    """One row per seller. The composite score (0-100) weights customer happiness
    (review), final-mile punctuality (late %) and the seller's own handoff speed
    (did they give the parcel to the carrier before shipping_limit_date?)."""
    fo = fact_orders.select("order_id", "is_late", "is_delivered", "review_score")
    x = fact_items.filter("is_sale").join(fo, "order_id", "left")
    g = x.groupBy("seller_id").agg(
        F.countDistinct("order_id").alias("orders"),
        F.count("*").alias("items"),
        F.round(F.sum("item_value"), 2).alias("gmv"),
        F.round(F.avg("review_score"), 2).alias("avg_review_score"),
        F.round(F.avg(F.when(F.col("is_delivered"), F.col("is_late").cast("double"))) * 100, 2)
        .alias("late_pct"),
        F.round(F.avg(F.when(F.col("carrier_ts").isNotNull(),
                             (F.col("carrier_ts") <= F.col("shipping_limit_ts")).cast("double"))) * 100, 2)
        .alias("handoff_on_time_pct"))
    g = g.filter(F.col("orders") >= min_orders)
    score = (0.4 * (F.coalesce("avg_review_score", F.lit(3.0)) - 1) / 4 * 100
             + 0.3 * (100 - F.coalesce("late_pct", F.lit(0.0)))
             + 0.3 * F.coalesce("handoff_on_time_pct", F.lit(100.0)))
    g = g.withColumn("score", F.round(score, 1))
    g = g.withColumn("tier", F.when(F.col("score") >= 85, "A")
                     .when(F.col("score") >= 70, "B").otherwise("C"))
    return (g.join(F.broadcast(dim_seller.select("seller_id", "seller_state", "seller_city")),
                   "seller_id", "left")
             .orderBy(F.col("gmv").desc()))


def agg_monthly_gmv(fact_orders: DataFrame) -> DataFrame:
    """GMV = sum of item price + freight over orders that count as sales."""
    m = (fact_orders.filter("is_sale")
         .withColumn("year_month", F.date_format("purchase_ts", "yyyy-MM"))
         .groupBy("year_month").agg(
             F.countDistinct("order_id").alias("orders"),
             F.round(F.sum("order_value"), 2).alias("gmv"),
             F.countDistinct("customer_unique_id").alias("customers")))
    w = Window.orderBy("year_month")
    return (m.withColumn("aov", F.round(F.col("gmv") / F.col("orders"), 2))
             .withColumn("prev_gmv", F.lag("gmv").over(w))
             .withColumn("mom_growth_pct",
                         F.round((F.col("gmv") - F.col("prev_gmv")) / F.col("prev_gmv") * 100, 2))
             .drop("prev_gmv").orderBy("year_month"))


def build_gold(spark: SparkSession, silver: dict[str, DataFrame]) -> dict[str, DataFrame]:
    s = silver
    orders, customers = s["olist_orders"], s["olist_customers"]
    fact_orders = build_fact_orders(orders, s["olist_order_items"], s["olist_order_payments"],
                                    s["olist_order_reviews"], customers)
    fact_items = build_fact_order_items(s["olist_order_items"], orders, customers)
    dim_customer = build_dim_customer(customers, orders)
    dim_seller = build_dim_seller(s["olist_sellers"])
    bounds = orders.agg(F.min(F.to_date("order_purchase_timestamp")).alias("lo"),
                        F.max(F.to_date("order_estimated_delivery_date")).alias("hi")).first()
    lo = bounds["lo"].replace(month=1, day=1)
    hi = bounds["hi"].replace(month=12, day=31)
    return {
        "dim_date": build_dim_date(spark, lo.isoformat(), hi.isoformat()),
        "dim_customer": dim_customer,
        "dim_product": build_dim_product(s["olist_products"], s["product_category_name_translation"]),
        "dim_seller": dim_seller,
        "fact_orders": fact_orders,
        "fact_order_items": fact_items,
        "fact_reviews": build_fact_reviews(s["olist_order_reviews"], orders, customers),
        "agg_delivery_by_state": agg_delivery_by_state(fact_orders, dim_customer),
        "agg_seller_scorecard": agg_seller_scorecard(fact_items, fact_orders, dim_seller),
        "agg_monthly_gmv": agg_monthly_gmv(fact_orders),
    }
