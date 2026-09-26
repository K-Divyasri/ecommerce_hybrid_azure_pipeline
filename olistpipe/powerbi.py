"""
Power BI support: export gold tables, compute the headline measures in pandas (to
cross-check the DAX in powerbi/measures.dax), and render matplotlib previews of the
three report pages.

The previews are NOT a Power BI report. They are rendered with matplotlib from the
same gold tables so the README has pictures; build the real report in Power BI Desktop
with powerbi/BUILD_GUIDE.md.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

EXPORT_TABLES = ["dim_date", "dim_customer", "dim_product", "dim_seller", "fact_orders",
                 "fact_order_items", "agg_delivery_by_state", "agg_seller_scorecard",
                 "agg_monthly_gmv", "customer_rfm", "daily_revenue_anomaly"]

PREVIEW_NOTE = ("preview rendered with matplotlib, build the real report in Power BI Desktop "
                "with powerbi/BUILD_GUIDE.md")

# validated categorical steps (blue, orange) + neutral inks, light surface
BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"
INK, INK2, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"
BLUES = ["#dbe8f8", "#a9c9ef", "#6ea3e3", "#2a78d6", "#1b56a0", "#10386b"]


def measures(fact_orders: pd.DataFrame) -> dict[str, float]:
    """The same four numbers as the DAX measures, computed the same way.
    GMV            sum(order_value) over sales (not canceled/unavailable)
    AOV            GMV / distinct sale orders
    On-time %      delivered orders not late / delivered orders
    Repeat rate    customers with 2+ sale orders / customers with 1+ sale order"""
    fo = fact_orders.copy()
    sale = fo["is_sale"].astype(int) == 1
    s = fo[sale]
    gmv = float(s["order_value"].astype(float).sum())
    orders = s["order_id"].nunique()
    delivered = fo[fo["is_delivered"].astype(int) == 1]
    on_time = 1 - delivered["is_late"].astype(int).mean() if len(delivered) else float("nan")
    per_cust = s.groupby("customer_unique_id")["order_id"].nunique()
    return {
        "gmv": round(gmv, 2),
        "orders": int(orders),
        "aov": round(gmv / orders, 2) if orders else float("nan"),
        "on_time_delivery_pct": round(100 * float(on_time), 2),
        "repeat_rate_pct": round(100 * float((per_cust >= 2).mean()), 2),
    }


def export(tables: dict[str, pd.DataFrame], out_dir: Path) -> dict[str, int]:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    counts = {}
    for name in EXPORT_TABLES:
        if name in tables:
            tables[name].to_csv(out_dir / f"{name}.csv", index=False)
            counts[name] = len(tables[name])
    return counts


def _style(ax, title: str):
    ax.set_title(title, loc="left", fontsize=11, color=INK, pad=8)
    ax.set_facecolor(SURFACE)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=INK2, labelsize=8)
    ax.grid(axis="y", color=GRID, linewidth=0.6)
    ax.set_axisbelow(True)


def _kpi(ax, label: str, value: str):
    ax.axis("off")
    ax.text(0.02, 0.62, value, fontsize=20, color=INK, weight="bold", transform=ax.transAxes)
    ax.text(0.02, 0.22, label, fontsize=9, color=INK2, transform=ax.transAxes)


def _footer(fig):
    fig.text(0.01, 0.005, PREVIEW_NOTE, fontsize=7, color=INK2)


def render_previews(t: dict[str, pd.DataFrame], out_dir: Path) -> list[Path]:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    fo = t["fact_orders"]
    m = measures(fo)
    paths = []

    # ---- page 1: sales overview ---------------------------------------------------
    fig = plt.figure(figsize=(12, 7), facecolor=SURFACE)
    gs = fig.add_gridspec(3, 4, height_ratios=[0.5, 1.6, 1.4], hspace=0.55, wspace=0.35)
    for i, (lab, val) in enumerate([("GMV (BRL)", f"{m['gmv']:,.0f}"), ("Orders", f"{m['orders']:,}"),
                                    ("Average order value", f"{m['aov']:,.2f}"),
                                    ("Repeat rate", f"{m['repeat_rate_pct']:.1f}%")]):
        _kpi(fig.add_subplot(gs[0, i]), lab, val)
    mg = t["agg_monthly_gmv"].sort_values("year_month")
    ax = fig.add_subplot(gs[1, :])
    x = np.arange(len(mg))
    ax.bar(x, mg["gmv"].astype(float), color=BLUE, width=0.7)
    ax.set_xticks(x[::2], mg["year_month"].iloc[::2], rotation=0, fontsize=7)
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v/1000:,.0f}k"))
    _style(ax, "Monthly GMV (BRL)")
    fi, dp = t["fact_order_items"], t["dim_product"]
    cat = (fi[fi["is_sale"].astype(int) == 1].merge(dp[["product_id", "category"]], on="product_id")
           .groupby("category")["item_value"].sum().astype(float).nlargest(10).sort_values())
    ax = fig.add_subplot(gs[2, :2])
    ax.barh(cat.index, cat.values, color=BLUE, height=0.65)
    ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v/1000:,.0f}k"))
    _style(ax, "Top 10 categories by GMV")
    ax.grid(axis="x", color=GRID, linewidth=0.6)
    ax.grid(axis="y", visible=False)
    an = t["daily_revenue_anomaly"].copy()
    an["day"] = pd.to_datetime(an["day"])
    ax = fig.add_subplot(gs[2, 2:])
    ax.plot(an["day"], an["revenue"].astype(float), color=BLUE, linewidth=1.2)
    flag = an[an["is_anomaly"].astype(int) == 1]
    ax.scatter(flag["day"], flag["revenue"].astype(float), color=ORANGE, s=28, zorder=3,
               edgecolor=SURFACE, linewidth=1.5, label="|z| > 3")
    ax.legend(frameon=False, fontsize=8, loc="upper left")
    ax.xaxis.set_major_locator(matplotlib.dates.MonthLocator(interval=4))
    ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%Y-%m"))
    _style(ax, "Daily revenue with z-score anomalies")
    fig.suptitle("Page 1 - Sales overview", x=0.01, ha="left", fontsize=14, color=INK)
    _footer(fig)
    p = out_dir / "page1_sales_overview.png"
    fig.savefig(p, dpi=110, bbox_inches="tight", facecolor=SURFACE)
    plt.close(fig)
    paths.append(p)

    # ---- page 2: delivery SLA by state ---------------------------------------------
    st = t["agg_delivery_by_state"].copy()
    st["late_pct"] = st["late_pct"].astype(float)
    st = st.sort_values("late_pct", ascending=True)
    fig = plt.figure(figsize=(12, 7), facecolor=SURFACE)
    gs = fig.add_gridspec(2, 3, height_ratios=[0.45, 3], hspace=0.35, wspace=0.4)
    _kpi(fig.add_subplot(gs[0, 0]), "On-time delivery", f"{m['on_time_delivery_pct']:.1f}%")
    delivered = fo[fo["is_delivered"].astype(int) == 1]
    _kpi(fig.add_subplot(gs[0, 1]), "Avg delivery days", f"{delivered['delivery_days'].astype(float).mean():.1f}")
    _kpi(fig.add_subplot(gs[0, 2]), "Delivered orders", f"{len(delivered):,}")
    ax = fig.add_subplot(gs[1, :2])
    bins = np.digitize(st["late_pct"], np.percentile(st["late_pct"], [20, 40, 60, 80]))
    ax.barh(st["customer_state"], st["late_pct"], color=[BLUES[min(b + 1, 5)] for b in bins], height=0.7)
    for y, (v, n) in enumerate(zip(st["late_pct"], st["delivered_orders"])):
        ax.text(v + 0.3, y, f"{v:.1f}%  (n={int(n)})", va="center", fontsize=7, color=INK2)
    _style(ax, "Late delivery % by customer state (map stand-in: Power BI uses a filled map)")
    ax.grid(axis="x", color=GRID, linewidth=0.6)
    ax.grid(axis="y", visible=False)
    ax = fig.add_subplot(gs[1, 2])
    ax.scatter(st["avg_delivery_days"].astype(float), st["avg_review_score"].astype(float),
               s=np.clip(st["delivered_orders"].astype(float) / 8, 10, 400), color=BLUE, alpha=0.75,
               edgecolor=SURFACE, linewidth=1.5)
    for _, r in st.nlargest(4, "delivered_orders").iterrows():
        ax.annotate(r["customer_state"], (float(r["avg_delivery_days"]), float(r["avg_review_score"])),
                    fontsize=8, color=INK2, xytext=(4, 4), textcoords="offset points")
    ax.set_xlabel("avg delivery days", fontsize=8, color=INK2)
    ax.set_ylabel("avg review score", fontsize=8, color=INK2)
    _style(ax, "Slower states review worse")
    fig.suptitle("Page 2 - Delivery SLA by state", x=0.01, ha="left", fontsize=14, color=INK)
    _footer(fig)
    p = out_dir / "page2_delivery_sla.png"
    fig.savefig(p, dpi=110, bbox_inches="tight", facecolor=SURFACE)
    plt.close(fig)
    paths.append(p)

    # ---- page 3: customer segments ------------------------------------------------
    from .rfm import SEGMENT_ORDER
    rfm = t["customer_rfm"]
    seg = rfm.groupby("segment").agg(customers=("customer_unique_id", "count"),
                                     revenue=("monetary", "sum"))
    seg = seg.reindex([s for s in SEGMENT_ORDER if s in seg.index])
    fig = plt.figure(figsize=(12, 7), facecolor=SURFACE)
    gs = fig.add_gridspec(1, 3, wspace=0.75)
    ax = fig.add_subplot(gs[0, 0])
    y = np.arange(len(seg))[::-1]
    ax.barh(y, seg["customers"], color=BLUE, height=0.65)
    ax.set_yticks(y, seg.index)
    for yy, v in zip(y, seg["customers"]):
        ax.text(v, yy, f" {int(v):,}", va="center", fontsize=7, color=INK2)
    _style(ax, "Customers per RFM segment")
    ax.grid(axis="y", visible=False)
    ax = fig.add_subplot(gs[0, 1])
    share = seg["revenue"].astype(float) / seg["revenue"].astype(float).sum() * 100
    ax.barh(y, share, color=AQUA, height=0.65)
    ax.set_yticks(y, seg.index)
    for yy, v in zip(y, share):
        ax.text(v, yy, f" {v:.1f}%", va="center", fontsize=7, color=INK2)
    _style(ax, "Share of revenue per segment")
    ax.grid(axis="y", visible=False)
    ax = fig.add_subplot(gs[0, 2])
    grid = rfm.pivot_table(index="r_score", columns="m_score", values="customer_unique_id",
                           aggfunc="count", fill_value=0).sort_index(ascending=False)
    cmap = matplotlib.colors.LinearSegmentedColormap.from_list("blues", BLUES)
    ax.imshow(grid.values, cmap=cmap, aspect="auto", vmin=0)
    ax.set_xticks(range(grid.shape[1]), grid.columns)
    ax.set_yticks(range(grid.shape[0]), grid.index)
    for i in range(grid.shape[0]):
        for j in range(grid.shape[1]):
            v = grid.values[i, j]
            ax.text(j, i, str(v), ha="center", va="center", fontsize=8,
                    color="white" if v > grid.values.max() * 0.55 else INK)
    ax.set_xlabel("M score (spend)", fontsize=8, color=INK2)
    ax.set_ylabel("R score (recency)", fontsize=8, color=INK2)
    ax.set_title("Customers by R x M score", loc="left", fontsize=11, color=INK)
    ax.tick_params(colors=INK2, labelsize=8)
    fig.suptitle("Page 3 - Customer segments (RFM)", x=0.01, ha="left", fontsize=14, color=INK)
    _footer(fig)
    p = out_dir / "page3_customer_segments.png"
    fig.savefig(p, dpi=110, bbox_inches="tight", facecolor=SURFACE)
    plt.close(fig)
    paths.append(p)
    return paths
