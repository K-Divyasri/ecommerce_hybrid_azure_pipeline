from datetime import date

import numpy as np
import pandas as pd
import pytest

from olistpipe.anomaly import daily_revenue, zscore_flags
from olistpipe.powerbi import measures
from olistpipe.rfm import assign_segment, quintile_score, rfm_table


def test_quintile_score_basic():
    v = np.arange(1, 101)
    s = quintile_score(v)
    assert s.min() == 1 and s.max() == 5
    assert (np.bincount(s)[1:] == 20).all()
    # lower is better for recency
    assert quintile_score(v, higher_is_better=False)[0] == 5


def test_frequency_collapse_with_ties():
    """90% one-time buyers: every percentile edge is 1, so 1 -> score 1, repeaters -> 5."""
    f = np.array([1] * 90 + [2] * 8 + [3] * 2)
    s = quintile_score(f)
    assert set(s[f == 1]) == {1}
    assert set(s[f > 1]) == {5}


def test_segments():
    seg = assign_segment(np.array([5, 1, 5, 1, 3]), np.array([5, 5, 1, 1, 1]),
                         np.array([5, 5, 2, 2, 3]))
    assert list(seg) == ["Champions", "Cannot Lose Them", "New Customers", "Lost", "Need Attention"]


def test_rfm_table_uses_as_of_not_today():
    orders = pd.DataFrame({
        "customer_unique_id": ["a", "a", "b", "c"],
        "order_id": ["1", "2", "3", "4"],
        "purchase_ts": ["2018-01-01", "2018-08-30", "2018-05-01", "2018-09-10"],
        "order_value": [100.0, 50.0, 20.0, 999.0],
    })
    r = rfm_table(orders, date(2018, 9, 1)).set_index("customer_unique_id")
    assert "c" not in r.index  # purchased after as_of: invisible
    assert r.loc["a", "recency_days"] == 2
    assert r.loc["a", "frequency"] == 2 and r.loc["a", "monetary"] == 150.0
    again = rfm_table(orders, date(2018, 9, 1))
    assert again.equals(rfm_table(orders, date(2018, 9, 1)))


def test_zscore_catches_spike_and_baseline_excludes_today():
    rng = np.random.default_rng(0)
    days = pd.date_range("2018-01-01", periods=60)
    rev = rng.normal(1000, 50, 60)
    rev[45] = 5000
    orders = pd.DataFrame({"purchase_ts": days, "order_value": rev})
    flags = zscore_flags(daily_revenue(orders))
    assert flags.loc[45, "is_anomaly"] and flags.loc[45, "direction"] == "spike"
    assert flags["is_anomaly"].sum() == 1
    # the day after the spike: the spike is now in the baseline, std is inflated
    assert flags.loc[46, "baseline_std"] > flags.loc[45, "baseline_std"] * 3


def test_missing_days_are_zero_revenue():
    orders = pd.DataFrame({"purchase_ts": ["2018-01-01", "2018-01-03"], "order_value": [10.0, 20.0]})
    d = daily_revenue(orders)
    assert len(d) == 3 and d.loc[1, "revenue"] == 0


def test_powerbi_measures_match_definitions():
    fo = pd.DataFrame({
        "order_id": ["o1", "o2", "o3", "o4"],
        "customer_unique_id": ["a", "a", "b", "c"],
        "order_value": [100.0, 50.0, 30.0, 999.0],
        "is_sale": [1, 1, 1, 0],
        "is_delivered": [1, 1, 1, 0],
        "is_late": [0, 1, 0, 0],
    })
    m = measures(fo)
    assert m["gmv"] == 180.0 and m["orders"] == 3 and m["aov"] == 60.0
    assert m["on_time_delivery_pct"] == pytest.approx(66.67, abs=0.01)
    assert m["repeat_rate_pct"] == 50.0  # a repeats, b does not; c had no sale
