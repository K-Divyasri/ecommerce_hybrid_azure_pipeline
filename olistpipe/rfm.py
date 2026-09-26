"""
RFM segmentation with numpy.

    R  recency    days since the customer's last order (smaller is better)
    F  frequency  number of orders
    M  monetary   total spend

Each is turned into a 1-5 score by cutting at the 20/40/60/80th percentiles
(np.percentile) and asking which bucket each value lands in (np.digitize).

The Olist twist: almost everyone buys exactly once, so the frequency percentiles are
ALL equal to 1. np.digitize then puts every one-time buyer in bucket 1 and every
repeat buyer in bucket 5. That is not a bug; it is the data telling you frequency
barely varies, which is why the segment rules below lean on R and M.
"""
from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd

SEGMENT_ORDER = [
    "Champions", "Loyal Customers", "Cannot Lose Them", "At Risk", "New Customers",
    "Potential Loyalists", "Need Attention", "Hibernating", "Lost",
]


def quintile_score(values: np.ndarray, higher_is_better: bool = True) -> np.ndarray:
    """Score 1..5 by percentile bucket. right=True means a value equal to an edge falls in
    the LOWER bucket, which keeps ties (like frequency == 1) together."""
    values = np.asarray(values, dtype=float)
    edges = np.percentile(values, [20, 40, 60, 80])
    score = np.digitize(values, edges, right=True) + 1
    return score if higher_is_better else 6 - score


def assign_segment(r: np.ndarray, f: np.ndarray, m: np.ndarray) -> np.ndarray:
    """Rules are checked top to bottom; the first match wins (np.select semantics)."""
    r, f, m = np.asarray(r), np.asarray(f), np.asarray(m)
    repeat = f >= 4
    conditions = [
        (r >= 4) & repeat & (m >= 4),
        (r >= 3) & repeat,
        (r == 1) & repeat & (m >= 5),
        (r <= 2) & (repeat | (m >= 5)),
        (r == 5) & ~repeat,
        (r == 4) & ~repeat,
        (r == 3) & ~repeat,
        (r == 2) & ~repeat,
        (r == 1) & ~repeat,
    ]
    return np.select(conditions, SEGMENT_ORDER, default="Need Attention")


def rfm_table(orders: pd.DataFrame, as_of: date) -> pd.DataFrame:
    """orders needs: customer_unique_id, order_id, purchase_ts, order_value.
    Pass only orders that count as sales (not canceled / unavailable).
    as_of is a parameter, never date.today(), so reruns give identical results."""
    df = orders.copy()
    df["purchase_ts"] = pd.to_datetime(df["purchase_ts"]).dt.tz_localize(None)
    as_of_ts = pd.Timestamp(as_of)
    df = df[df["purchase_ts"] < as_of_ts]
    g = df.groupby("customer_unique_id").agg(
        last_order_ts=("purchase_ts", "max"),
        first_order_ts=("purchase_ts", "min"),
        frequency=("order_id", "nunique"),
        monetary=("order_value", "sum"),
    ).reset_index()
    g["recency_days"] = (as_of_ts - g["last_order_ts"]).dt.days
    g["r_score"] = quintile_score(g["recency_days"].to_numpy(), higher_is_better=False)
    g["f_score"] = quintile_score(g["frequency"].to_numpy())
    g["m_score"] = quintile_score(g["monetary"].to_numpy())
    g["rfm_code"] = (g["r_score"].astype(str) + g["f_score"].astype(str) + g["m_score"].astype(str))
    g["segment"] = assign_segment(g["r_score"], g["f_score"], g["m_score"])
    g["monetary"] = g["monetary"].round(2)
    g["as_of"] = as_of_ts.date().isoformat()
    return g[["customer_unique_id", "recency_days", "frequency", "monetary", "r_score", "f_score",
              "m_score", "rfm_code", "segment", "first_order_ts", "last_order_ts", "as_of"]]


def segment_summary(rfm: pd.DataFrame) -> pd.DataFrame:
    s = rfm.groupby("segment").agg(customers=("customer_unique_id", "count"),
                                   avg_recency=("recency_days", "mean"),
                                   avg_frequency=("frequency", "mean"),
                                   avg_monetary=("monetary", "mean"),
                                   total_monetary=("monetary", "sum"))
    s["share_customers"] = s["customers"] / s["customers"].sum()
    s["share_revenue"] = s["total_monetary"] / s["total_monetary"].sum()
    order = [x for x in SEGMENT_ORDER if x in s.index]
    return s.loc[order].round(3)
