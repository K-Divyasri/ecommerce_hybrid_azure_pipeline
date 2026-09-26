"""
Make the "live store" feel live.

One call = one simulated business day. It behaves like the shop's own application:
  * new orders arrive (some from returning customers, which in Olist means a NEW
    customer_id row pointing at the SAME customer_unique_id)
  * in-flight orders move forward: created -> approved -> shipped -> delivered
  * a fixed slice of shipments is "stuck" at the carrier and only arrives after the
    estimated date, so late deliveries keep appearing day after day
  * delivered orders get reviews, a few approved orders get canceled
Every change stamps updated_at with the simulated time, which is exactly what the
incremental extract keys on. Same day + same seed = same changes.
"""
from __future__ import annotations

import hashlib
import random
from datetime import date, datetime, timedelta

import numpy as np
from sqlalchemy.engine import Engine

from .db import TS_FMT, execute, query_df
from .generator import CITIES, CUSTOMER_STATE_WEIGHTS, REVIEW_TEXT, ZIP_RANGE, _hex_id

IN_FLIGHT = ("created", "approved", "invoiced", "processing", "shipped")


def _stuck(order_id: str) -> bool:
    """About 1 in 6 shipments get stuck at the carrier. Derived from the id so the
    decision is stable across days without storing any extra state."""
    return int(hashlib.md5(order_id.encode()).hexdigest(), 16) % 6 == 0


def simulate_day(engine: Engine, day: date, new_orders: int = 15, seed: int = 7) -> dict[str, int]:
    rng = np.random.default_rng([seed, day.toordinal()])
    rnd = random.Random(seed * 1_000_003 + day.toordinal())
    day0 = datetime(day.year, day.month, day.day)
    # sorted event times through the day: every change gets a strictly later timestamp
    clock = iter(sorted(day0 + timedelta(seconds=float(s))
                        for s in rng.uniform(60, 86_000, 20_000)))

    def tick() -> str:
        return next(clock).strftime(TS_FMT)

    def plain(ts: str) -> str:
        return ts[:19]

    already = query_df(engine, """SELECT COUNT(*) AS n FROM olist_orders
        WHERE order_purchase_timestamp >= :a AND order_purchase_timestamp < :b""",
                       {"a": day0.strftime("%Y-%m-%d %H:%M:%S"),
                        "b": (day0 + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")})
    if int(already["n"].iloc[0]) > 0:
        # the shop already traded that day: replaying it would invent a parallel universe
        return {"skipped_day_already_has_orders": int(already["n"].iloc[0])}

    stats = {"new_orders": 0, "approved": 0, "shipped": 0, "delivered": 0, "late_delivered": 0,
             "reviews": 0, "canceled": 0, "returning_customers": 0}

    # ---- move in-flight orders forward --------------------------------------------
    open_orders = query_df(engine, f"""
        SELECT order_id, order_status, order_purchase_timestamp, order_estimated_delivery_date
        FROM olist_orders
        WHERE order_status IN ({', '.join(repr(s) for s in IN_FLIGHT)})
          AND order_purchase_timestamp < :day0
        ORDER BY order_id""", {"day0": day0.strftime(TS_FMT)})
    for row in open_orders.itertuples(index=False):
        status, oid = row.order_status, row.order_id
        r = rng.random()
        if status == "created":
            if r < 0.9:
                ts = tick()
                execute(engine, "UPDATE olist_orders SET order_status='approved', order_approved_at=:t,"
                        " updated_at=:u WHERE order_id=:o", {"t": plain(ts), "u": ts, "o": oid})
                stats["approved"] += 1
        elif status in ("approved", "invoiced", "processing"):
            if r < 0.02:
                ts = tick()
                execute(engine, "UPDATE olist_orders SET order_status='canceled', updated_at=:u "
                        "WHERE order_id=:o", {"u": ts, "o": oid})
                stats["canceled"] += 1
            elif r < 0.65:
                ts = tick()
                execute(engine, "UPDATE olist_orders SET order_status='shipped', "
                        "order_delivered_carrier_date=:t, updated_at=:u WHERE order_id=:o",
                        {"t": plain(ts), "u": ts, "o": oid})
                stats["shipped"] += 1
        elif status == "shipped":
            estimated = np.datetime64(str(row.order_estimated_delivery_date)[:10])
            overdue = np.datetime64(day.isoformat()) > estimated
            deliver = (r < 0.35) if not _stuck(oid) else (overdue and r < 0.5)
            if deliver:
                ts = tick()
                execute(engine, "UPDATE olist_orders SET order_status='delivered', "
                        "order_delivered_customer_date=:t, updated_at=:u WHERE order_id=:o",
                        {"t": plain(ts), "u": ts, "o": oid})
                stats["delivered"] += 1
                stats["late_delivered"] += int(overdue)
                _add_review(engine, rng, rnd, oid, day0, tick(), late=bool(overdue))
                stats["reviews"] += 1

    # ---- brand new orders ----------------------------------------------------------
    catalog = query_df(engine, """
        SELECT product_id, MIN(seller_id) AS seller_id, AVG(price) AS price,
               AVG(freight_value) AS freight
        FROM olist_order_items WHERE price > 0
        GROUP BY product_id ORDER BY product_id""")
    people = query_df(engine, """
        SELECT customer_unique_id, MIN(customer_zip_code_prefix) AS zip,
               MIN(customer_city) AS city, MIN(customer_state) AS state
        FROM olist_customers GROUP BY customer_unique_id ORDER BY customer_unique_id""")
    states = list(CUSTOMER_STATE_WEIGHTS)
    sp = np.array([CUSTOMER_STATE_WEIGHTS[s] for s in states])
    sp = sp / sp.sum()

    for _ in range(new_orders):
        purchase = tick()
        if len(people) and rng.random() < 0.25:
            p = people.iloc[int(rng.integers(0, len(people)))]
            uid, zipc, city, st = p.customer_unique_id, p.zip, p.city, p.state
            stats["returning_customers"] += 1
        else:
            st = states[rng.choice(len(states), p=sp)]
            lo, hi = ZIP_RANGE[st]
            uid, zipc = _hex_id(rnd), f"{int(rng.integers(lo, hi)):05d}"
            city = CITIES[st][int(rng.integers(0, len(CITIES[st])))]
        cid, oid = _hex_id(rnd), _hex_id(rnd)
        execute(engine, """INSERT INTO olist_customers (customer_id, customer_unique_id,
            customer_zip_code_prefix, customer_city, customer_state, updated_at)
            VALUES (:c, :u, :z, :city, :st, :ts)""",
                {"c": cid, "u": uid, "z": zipc, "city": city, "st": st, "ts": purchase})
        # 30% of orders get an "express" promise (4-8 days); with stuck shipments those
        # are the ones that turn up late within a week of simulation
        promise = int(rng.integers(4, 9)) if rng.random() < 0.3 else int(rng.integers(12, 30))
        est = (day0 + timedelta(days=promise)).strftime("%Y-%m-%d %H:%M:%S")
        execute(engine, """INSERT INTO olist_orders (order_id, customer_id, order_status,
            order_purchase_timestamp, order_estimated_delivery_date, updated_at)
            VALUES (:o, :c, 'created', :p, :e, :ts)""",
                {"o": oid, "c": cid, "p": plain(purchase), "e": est, "ts": purchase})
        n_items = int(rng.choice([1, 2, 3], p=[0.9, 0.08, 0.02]))
        total = 0.0
        prod = catalog.iloc[int(rng.integers(0, len(catalog)))]
        for i in range(n_items):
            price = round(float(prod.price) * float(rng.uniform(0.95, 1.05)), 2)
            freight = round(float(prod.freight), 2)
            total += price + freight
            execute(engine, """INSERT INTO olist_order_items (order_id, order_item_id, product_id,
                seller_id, shipping_limit_date, price, freight_value, updated_at)
                VALUES (:o, :i, :p, :s, :lim, :pr, :fr, :ts)""",
                    {"o": oid, "i": i + 1, "p": prod.product_id, "s": prod.seller_id,
                     "lim": (day0 + timedelta(days=6)).strftime("%Y-%m-%d %H:%M:%S"),
                     "pr": price, "fr": freight, "ts": purchase})
        ptype = str(rng.choice(["credit_card", "boleto", "debit_card"], p=[0.78, 0.19, 0.03]))
        execute(engine, """INSERT INTO olist_order_payments (order_id, payment_sequential,
            payment_type, payment_installments, payment_value, updated_at)
            VALUES (:o, 1, :t, :n, :v, :ts)""",
                {"o": oid, "t": ptype, "n": int(rng.integers(1, 9)) if ptype == "credit_card" else 1,
                 "v": round(total, 2), "ts": purchase})
        stats["new_orders"] += 1
    return stats


def _add_review(engine: Engine, rng, rnd, order_id: str, day0: datetime, ts: str, late: bool) -> None:
    p = [0.46, 0.12, 0.14, 0.12, 0.16] if late else [0.07, 0.025, 0.075, 0.20, 0.63]
    score = int(rng.choice([1, 2, 3, 4, 5], p=p))
    msg = None
    if rng.random() < 0.5:
        options = REVIEW_TEXT[score]
        msg = options[int(rng.integers(0, len(options)))]
    execute(engine, """INSERT INTO olist_order_reviews (review_id, order_id, review_score,
        review_comment_title, review_comment_message, review_creation_date,
        review_answer_timestamp, updated_at)
        VALUES (:r, :o, :s, NULL, :m, :c, :a, :ts)""",
            {"r": _hex_id(rnd), "o": order_id, "s": score, "m": msg,
             "c": day0.strftime("%Y-%m-%d %H:%M:%S"), "a": ts[:19], "ts": ts})
