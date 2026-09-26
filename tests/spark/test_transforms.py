from datetime import datetime as dt

import pytest

from spark_jobs import dq
from spark_jobs.transforms import (agg_monthly_gmv, build_dim_customer, build_dim_product,
                                   build_fact_orders, latest_version)


def test_latest_version_keeps_newest(spark):
    df = spark.createDataFrame([
        (1, "o1", "created", dt(2018, 9, 1, 10), "2018-09-01"),
        (1, "o1", "shipped", dt(2018, 9, 2, 10), "2018-09-02"),
        (1, "o1", "delivered", dt(2018, 9, 5, 10), "2018-09-05"),
        (2, "o2", "created", dt(2018, 9, 1, 11), "2018-09-01"),
    ], "row_id long, order_id string, order_status string, updated_at timestamp, ingest_date string")
    out = {r.order_id: r.order_status for r in latest_version(df, ["order_id"]).collect()}
    assert out == {"o1": "delivered", "o2": "created"}


def test_latest_version_row_id_breaks_timestamp_tie(spark):
    df = spark.createDataFrame([
        (10, "k", "old", dt(2018, 9, 1)), (11, "k", "new", dt(2018, 9, 1))],
        "row_id long, key string, v string, updated_at timestamp")
    assert latest_version(df, ["key"]).first().v == "new"


def test_dim_customer_is_one_row_per_person(spark):
    customers = spark.createDataFrame([
        ("c1", "u1", "01001", "sao paulo", "SP"), ("c2", "u1", "20000", "rio de janeiro", "RJ"),
        ("c3", "u2", "50000", "recife", "PE")],
        "customer_id string, customer_unique_id string, customer_zip_code_prefix string, "
        "customer_city string, customer_state string")
    orders = spark.createDataFrame([("c1", dt(2018, 1, 1)), ("c2", dt(2018, 6, 1)), ("c3", dt(2018, 2, 1))],
                                   "customer_id string, order_purchase_timestamp timestamp")
    d = {r.customer_unique_id: r for r in build_dim_customer(customers, orders).collect()}
    assert len(d) == 2
    assert d["u1"].customer_state == "RJ"  # address from the most recent order
    assert d["u1"].first_order_ts == dt(2018, 1, 1)


def test_fact_orders_no_fan_out(spark):
    """3 items x 2 payments must NOT become 6 rows or triple-count payments."""
    orders = spark.createDataFrame(
        [("o1", "c1", "delivered", dt(2018, 1, 1), dt(2018, 1, 12), dt(2018, 1, 10))],
        "order_id string, customer_id string, order_status string, order_purchase_timestamp timestamp, "
        "order_delivered_customer_date timestamp, order_estimated_delivery_date timestamp")
    items = spark.createDataFrame([("o1", i, "p", "s", 10.0, 2.0) for i in (1, 2, 3)],
                                  "order_id string, order_item_id int, product_id string, seller_id string, "
                                  "price double, freight_value double")
    pays = spark.createDataFrame([("o1", 1, "voucher", 1, 6.0), ("o1", 2, "credit_card", 3, 30.0)],
                                 "order_id string, payment_sequential int, payment_type string, "
                                 "payment_installments int, payment_value double")
    reviews = spark.createDataFrame([("r1", "o1", 3)], "review_id string, order_id string, review_score int")
    cust = spark.createDataFrame([("c1", "u1")], "customer_id string, customer_unique_id string")
    rows = build_fact_orders(orders, items, pays, reviews, cust).collect()
    assert len(rows) == 1
    r = rows[0]
    assert r.item_count == 3 and r.order_value == 36.0 and r.payment_value == 36.0
    assert r.main_payment_type == "credit_card"
    assert r.is_late and r.delay_days == 2 and r.is_sale


def test_dim_product_untranslated_category_falls_back(spark):
    products = spark.createDataFrame([
        ("p1", "pc_gamer", 1, 500, 10, 10, 10), ("p2", "beleza_saude", 2, 100, 5, 5, 5),
        ("p3", None, None, 50, 1, 1, 1)],
        "product_id string, product_category_name string, product_photos_qty int, "
        "product_weight_g int, product_length_cm int, product_height_cm int, product_width_cm int")
    tr = spark.createDataFrame([("beleza_saude", "health_beauty")],
                               "product_category_name string, product_category_name_english string")
    d = {r.product_id: r.category for r in build_dim_product(products, tr).collect()}
    assert d == {"p1": "pc_gamer", "p2": "health_beauty", "p3": "unknown"}


def test_monthly_gmv_excludes_canceled(spark):
    fo = spark.createDataFrame([
        ("o1", "u1", dt(2018, 1, 5), 100.0, True), ("o2", "u2", dt(2018, 1, 9), 50.0, False),
        ("o3", "u1", dt(2018, 2, 1), 150.0, True)],
        "order_id string, customer_unique_id string, purchase_ts timestamp, order_value double, is_sale boolean")
    m = {r.year_month: r for r in agg_monthly_gmv(fo).collect()}
    assert m["2018-01"].gmv == 100.0 and m["2018-01"].orders == 1
    assert m["2018-02"].mom_growth_pct == 50.0


def test_dq_split_quarantines_and_null_counts_as_fail(spark):
    df = spark.createDataFrame([("o1", 1, 10.0, 1.0, "p", "s"), ("o1", 2, -5.0, 1.0, "p", "s"),
                                ("o2", 1, None, 1.0, "p", None)],
                               "order_id string, order_item_id int, price double, freight_value double, "
                               "product_id string, seller_id string")
    good, bad = dq.split(df, dq.rules()["olist_order_items"])
    assert good.count() == 1
    reasons = {r.order_item_id if r.order_id == "o1" else "o2": r.dq_failed_rules for r in bad.collect()}
    assert reasons[2] == "price_positive"
    assert reasons["o2"] == "price_positive,seller_id_not_null"


def test_gate_raises_above_threshold():
    ok = [dq.TableResult("t", 1000, 5)]
    dq.gate(ok, 0.02, log=lambda *a: None)
    with pytest.raises(dq.DataQualityError):
        dq.gate([dq.TableResult("t", 100, 5)], 0.02, log=lambda *a: None)
