"""
Flag unusual days of revenue with a z-score.

    z = (today - mean of the previous N days) / standard deviation of the previous N days

"Previous" matters: if today were included in its own baseline, a huge spike would
inflate the mean and the std it is compared against and partly hide itself. So the
rolling window is shifted by one day.

|z| > 3 means "further from normal than 3 standard deviations", which for roughly
bell-shaped data happens by chance about 0.3% of the time.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def daily_revenue(orders: pd.DataFrame, start=None, end=None) -> pd.DataFrame:
    """orders needs purchase_ts and order_value (sales only). Missing days become 0
    revenue: a day with no sales is a data point, not a gap to skip."""
    df = orders.copy()
    df["day"] = pd.to_datetime(df["purchase_ts"]).dt.tz_localize(None).dt.normalize()
    daily = df.groupby("day").agg(revenue=("order_value", "sum"), orders=("order_value", "size"))
    idx = pd.date_range(start or daily.index.min(), end or daily.index.max(), freq="D")
    daily = daily.reindex(idx, fill_value=0)
    daily.index.name = "day"
    return daily.reset_index()


def zscore_flags(daily: pd.DataFrame, window: int = 28, threshold: float = 3.0,
                 min_periods: int = 14) -> pd.DataFrame:
    out = daily.copy()
    rev = out["revenue"].astype(float)
    baseline = rev.shift(1).rolling(window, min_periods=min_periods)
    out["baseline_mean"] = baseline.mean()
    out["baseline_std"] = baseline.std(ddof=1)
    std = out["baseline_std"].replace(0, np.nan)
    out["z_score"] = (rev - out["baseline_mean"]) / std
    out["is_anomaly"] = out["z_score"].abs() > threshold
    out["direction"] = np.where(out["is_anomaly"], np.where(out["z_score"] > 0, "spike", "drop"), "")
    # global z-score for comparison: one mean/std for the whole history
    out["z_score_global"] = (rev - rev.mean()) / rev.std(ddof=1)
    return out.round({"revenue": 2, "baseline_mean": 2, "baseline_std": 2, "z_score": 3,
                      "z_score_global": 3})
