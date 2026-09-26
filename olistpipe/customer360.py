"""
Build "customer 360" JSON documents and review documents, then sync them to Cosmos.

A customer 360 document answers one question in ONE read: "tell me everything about
this customer". In MySQL that is five joins; in Cosmos it is a point read costing about
1 RU because the answer was pre-assembled (denormalised) at write time.

Sync is incremental and idempotent: each document's content hash is remembered in the
etl_cosmos_sync table, and only documents whose hash changed are upserted. Rerunning
the same day upserts nothing and spends no RUs.
"""
from __future__ import annotations

import hashlib
import json
from datetime import date

import pandas as pd
from sqlalchemy.engine import Engine

from .db import execute, query_df

SALES_EXCLUDED = ("canceled", "unavailable")


def _iso(v) -> str | None:
    if v is None or pd.isna(v):
        return None
    ts = pd.Timestamp(v)
    return (ts.tz_localize(None) if ts.tzinfo else ts).isoformat()


def build_customer_docs(fact_orders: pd.DataFrame, dim_customer: pd.DataFrame,
                        rfm: pd.DataFrame, reviews: pd.DataFrame, as_of: date,
                        recent_reviews: int = 3) -> list[dict]:
    fo = fact_orders.sort_values("order_id").reset_index(drop=True)  # stable float sums
    fo["purchase_ts"] = pd.to_datetime(fo["purchase_ts"]).dt.tz_localize(None)
    sales = fo[~fo["order_status"].isin(SALES_EXCLUDED)]
    agg = sales.groupby("customer_unique_id").agg(
        order_count=("order_id", "nunique"), total_spend=("order_value", "sum"),
        first_order=("purchase_ts", "min"), last_order=("purchase_ts", "max"),
        late_deliveries=("is_late", "sum"))
    last_order_id = (sales.sort_values(["purchase_ts", "order_id"])
                     .groupby("customer_unique_id").tail(1).set_index("customer_unique_id"))
    rv = reviews.copy()
    rv["review_creation_ts"] = pd.to_datetime(rv["review_creation_ts"]).dt.tz_localize(None)
    rv = rv.sort_values(["review_creation_ts", "review_id"], ascending=[False, True])
    rv_by_cust = {k: g.head(recent_reviews) for k, g in rv.groupby("customer_unique_id")}
    rfm_i = rfm.set_index("customer_unique_id")

    docs = []
    for c in dim_customer.itertuples(index=False):
        uid = c.customer_unique_id
        if uid not in agg.index:
            continue  # customer with only canceled orders: no 360 yet
        a = agg.loc[uid]
        lo = last_order_id.loc[uid]
        doc = {
            "id": uid,
            "customer_unique_id": uid,
            "type": "customer360",
            "profile": {"city": c.customer_city, "state": c.customer_state,
                        "zip_code_prefix": c.customer_zip_code_prefix},
            "order_count": int(a.order_count),
            "total_spend": round(float(a.total_spend), 2),
            "avg_order_value": round(float(a.total_spend) / int(a.order_count), 2),
            "first_order_date": a.first_order.date().isoformat(),
            "last_order": {"order_id": lo.order_id, "date": lo.purchase_ts.date().isoformat(),
                           "value": round(float(lo.order_value), 2), "status": lo.order_status},
            "late_deliveries": int(a.late_deliveries),
            "recent_reviews": [],
        }
        if uid in rfm_i.index:
            r = rfm_i.loc[uid]
            doc["rfm"] = {"r": int(r.r_score), "f": int(r.f_score), "m": int(r.m_score),
                          "code": str(r.rfm_code), "segment": str(r.segment)}
        if uid in rv_by_cust:
            for x in rv_by_cust[uid].itertuples(index=False):
                doc["recent_reviews"].append({
                    "review_id": x.review_id, "order_id": x.order_id, "score": int(x.review_score),
                    "title": None if pd.isna(x.review_comment_title) else x.review_comment_title,
                    "message": None if pd.isna(x.review_comment_message) else x.review_comment_message,
                    "created": x.review_creation_ts.date().isoformat()})
        doc["as_of"] = as_of.isoformat()
        docs.append(doc)
    return docs


def build_review_docs(reviews: pd.DataFrame) -> list[dict]:
    """One document per review. id = review_id, partition key = order_id. In real Olist a
    handful of review_ids repeat across orders; Cosmos ids only need to be unique WITHIN a
    partition, so (order_id, review_id) never collides."""
    docs = []
    for x in reviews.itertuples(index=False):
        docs.append({
            "id": x.review_id, "order_id": x.order_id, "type": "review",
            "customer_unique_id": x.customer_unique_id, "score": int(x.review_score),
            "title": None if pd.isna(x.review_comment_title) else x.review_comment_title,
            "message": None if pd.isna(x.review_comment_message) else x.review_comment_message,
            "created": _iso(x.review_creation_ts),
        })
    return docs


def doc_hash(doc: dict) -> str:
    body = {k: v for k, v in doc.items() if k not in ("as_of", "_rid", "_self", "_etag", "_ts",
                                                      "_attachments")}
    return hashlib.sha256(json.dumps(body, sort_keys=True, default=str).encode()).hexdigest()


def sync_docs(store, sync_engine: Engine, container: str, docs: list[dict], log=print) -> dict:
    """Upsert only documents whose content changed since the last sync."""
    known = query_df(sync_engine, "SELECT doc_id, doc_hash FROM etl_cosmos_sync WHERE container=:c",
                     {"c": container})
    known = dict(zip(known["doc_id"], known["doc_hash"])) if len(known) else {}
    changed = [(d, doc_hash(d)) for d in docs if known.get(d["id"]) != doc_hash(d)]
    ru_total, ru_seen = 0.0, False
    for d, _h in changed:
        ru = store.upsert(container, d)
        if ru is not None:
            ru_total += ru
            ru_seen = True
    # remember hashes only after the upserts succeeded; if an upsert crashed above, the
    # next run simply re-sends those docs (upsert is idempotent)
    rows = [{"i": d["id"], "c": container, "h": h} for d, h in changed]
    if rows:
        execute(sync_engine, "DELETE FROM etl_cosmos_sync WHERE container=:c AND doc_id=:i", rows)
        execute(sync_engine, "INSERT INTO etl_cosmos_sync (doc_id, container, doc_hash) "
                "VALUES (:i, :c, :h)", rows)
    result = {"container": container, "documents": len(docs), "upserted": len(changed),
              "unchanged": len(docs) - len(changed),
              "ru_total": round(ru_total, 2) if ru_seen else None}
    log(f"[cosmos] {container}: {result}")
    return result
