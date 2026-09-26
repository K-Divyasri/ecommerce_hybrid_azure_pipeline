# Building the Power BI report

This guide builds a three-page report in **Power BI Desktop** (free, Windows only,
install it from the Microsoft Store) from the gold tables the pipeline exports.

Why a guide and not a `.pbix` file: a `.pbix` is a binary you build by clicking in
Power BI Desktop, it cannot be generated honestly from code. Publishing to the
Power BI Service (the website) needs a work or school email; Gmail and Outlook.com
addresses are refused. So the portfolio version of this report is: this guide,
`measures.dax`, `theme.json`, and **your own screenshots** of the pages you build.

The PNGs in `previews/` are matplotlib stand-ins rendered from the same data so the
README has pictures. They are not Power BI.

## What is in this folder

| File | What it is |
|------|------------|
| `gold_exports/*.csv` | the star schema + aggregates, exported from the MySQL `analytics` database by the `export_powerbi` task |
| `measures.dax` | every DAX measure used below |
| `theme.json` | colors and fonts |
| `previews/*.png` | matplotlib previews of the three pages |

## 1. Load the data

1. Open Power BI Desktop > **Get data** > **Text/CSV**.
2. Load these files from `powerbi\gold_exports\` one at a time (or use
   **Get data > Folder** and filter, if you are comfortable with Power Query):

   | Table | Columns |
   |-------|---------|
   | `fact_orders` | order_id, customer_unique_id, purchase_date_key, purchase_ts, order_status, item_count, seller_count, items_value, freight_value, order_value, payment_value, main_payment_type, max_installments, delivered_ts, estimated_ts, is_delivered, delivery_days, delay_days, is_late, review_score, is_sale |
   | `fact_order_items` | order_id, order_item_id, product_id, seller_id, customer_unique_id, purchase_date_key, order_status, price, freight_value, item_value, shipping_limit_ts, carrier_ts, is_sale |
   | `dim_date` | date_key, date, year, quarter, month, month_name, year_month, day, day_name, is_weekend |
   | `dim_customer` | customer_unique_id, customer_city, customer_state, customer_zip_code_prefix, first_order_ts, order_ids_seen |
   | `dim_product` | product_id, category_pt, category, photos_qty, weight_g, volume_cm3 |
   | `dim_seller` | seller_id, seller_city, seller_state, seller_zip_code_prefix |
   | `customer_rfm` | customer_unique_id, recency_days, frequency, monetary, r_score, f_score, m_score, rfm_code, segment, first_order_ts, last_order_ts, as_of |
   | `daily_revenue_anomaly` | day, revenue, orders, baseline_mean, baseline_std, z_score, is_anomaly, direction, z_score_global |
   | `agg_delivery_by_state` | customer_state, delivered_orders, late_orders, late_pct, avg_delivery_days, avg_days_late_when_late, avg_review_score |
   | `agg_monthly_gmv` | year_month, orders, gmv, customers, aov, mom_growth_pct |
   | `agg_seller_scorecard` | seller_id, orders, items, gmv, avg_review_score, late_pct, handoff_on_time_pct, score, tier, seller_state, seller_city |

3. In each preview click **Transform Data** if a type looks wrong. Check these in
   Power Query:
   - every `*_id` and `customer_zip_code_prefix` / `seller_zip_code_prefix`: **Text**
     (otherwise zip `01001` becomes the number 1001)
   - `date_key`, `purchase_date_key`: **Whole number**
   - `dim_date[date]`, `daily_revenue_anomaly[day]`: **Date**
   - `*_ts`: **Date/Time**
   - money columns (`order_value`, `price`, `gmv`, `monetary`, ...): **Decimal number**
     (or Fixed decimal)
   - `is_sale`, `is_delivered`, `is_late`, `is_anomaly`: **Whole number** (they are 0/1;
     the DAX compares them to 1)
4. **Close & Apply**.

## 2. Mark the date table

Select `dim_date` in the Data pane > **Table tools** > **Mark as date table** > choose
the `date` column. Time intelligence functions (`DATEADD` in `GMV MoM %`) need this.

## 3. Relationships (Model view)

Delete anything Power BI auto-detected that is not on this list, then create:

| From (many side) | To (one side) | Cardinality | Filter direction |
|------------------|---------------|-------------|------------------|
| `fact_orders[purchase_date_key]` | `dim_date[date_key]` | many to one | single |
| `fact_orders[customer_unique_id]` | `dim_customer[customer_unique_id]` | many to one | single |
| `fact_order_items[product_id]` | `dim_product[product_id]` | many to one | single |
| `fact_order_items[seller_id]` | `dim_seller[seller_id]` | many to one | single |
| `fact_order_items[purchase_date_key]` | `dim_date[date_key]` | many to one | single |
| `customer_rfm[customer_unique_id]` | `dim_customer[customer_unique_id]` | one to one | single (dim_customer filters customer_rfm) |

Leave the `agg_*` tables and `daily_revenue_anomaly` unrelated; they are already
aggregated and are used in their own visuals. You get a star: two fact tables in the
middle, dimensions around them, all filters flowing from dimensions to facts.

## 4. Theme

**View** > **Themes** > **Browse for themes** > pick `powerbi\theme.json`.

## 5. Measures

**Home** > **Enter data**, name the table `_Measures`, **Load**. Select it and add each
measure from `measures.dax` with **New measure** (paste one block at a time). Set
formats in **Measure tools**:

- `GMV`, `AOV`, `GMV Previous Month`, `Anomaly Revenue`: currency or decimal, 0-2 decimals
- `Orders`, `Customers`, `Delivered Orders`, `Late Orders`, `Segment Customers`: whole number
- `Repeat Rate %`, `Late %`, `On-time Delivery %`, `GMV MoM %`, `Segment Revenue Share %`: **Percentage**
- `Avg Delivery Days`, `Avg Days Late When Late`, `Avg Review Score`: 1-2 decimals

## 6. Page 1: Sales overview

Rename the page "Sales overview".

1. **Four Card visuals** across the top: `GMV`, `Orders`, `AOV`, `Repeat Rate %`.
2. **Clustered column chart**: X axis `dim_date[year_month]`, Y axis `GMV`. Sort by
   `year_month` ascending (the "..." menu > Sort axis).
3. **Clustered bar chart**: Y axis `dim_product[category]`, X axis
   `SUM(fact_order_items[item_value])`, add a visual-level filter
   `fact_order_items[is_sale] = 1` and a **Top N** filter on category (top 10 by
   item_value).
4. **Line chart**: X axis `daily_revenue_anomaly[day]`, Y axis
   `SUM(daily_revenue_anomaly[revenue])` and `Anomaly Revenue`. Format the
   `Anomaly Revenue` series: line width 0, markers on, orange. The orange dots are the
   days with |z| > 3.
5. Optional slicer: `dim_date[year]`.

## 7. Page 2: Delivery SLA by state

1. Cards: `On-time Delivery %`, `Avg Delivery Days`, `Delivered Orders`.
2. **Filled map**: Location `dim_customer[customer_state]`, color saturation `Late %`.
   - Select `customer_state` in the Data pane > **Column tools** > **Data category** >
     **State or Province**.
   - Two-letter codes like `SP`, `RJ`, `MA` are ambiguous worldwide (`MA` is also
     Massachusetts), so Bing can place them in the wrong country. Fix it one of two ways:
     add a calculated column `state_geo = "BR-" & dim_customer[customer_state]` (ISO
     3166-2 codes) and use that as Location, or add a column `country = "Brazil"` and put
     Country above State in the Location well.
   - If the Filled map visual is disabled, enable it: **File > Options and settings >
     Options > Security > Use Map and Filled Map visuals**.
3. **Table**: all columns of `agg_delivery_by_state`, sorted by `late_pct` descending.
   Add conditional formatting (background color) on `late_pct`.
4. **Scatter chart**: Values `dim_customer[customer_state]`, X `Avg Delivery Days`,
   Y `Avg Review Score`, Size `Delivered Orders`. The story: slower states review worse.

## 8. Page 3: Customer segments (RFM)

1. **Slicer**: `customer_rfm[segment]`.
2. **Clustered bar chart**: Y `customer_rfm[segment]`, X `Segment Customers`.
3. **Clustered bar chart**: Y `customer_rfm[segment]`, X `Segment Revenue Share %`.
4. **Matrix**: Rows `customer_rfm[r_score]` (sort descending so 5 is on top), Columns
   `customer_rfm[m_score]`, Values `Segment Customers`. Then **Format > Cell elements >
   Background color** on, gradient from `#dbe8f8` (lowest) to `#10386b` (highest). That
   turns the matrix into an R x M heatmap.
5. Optional **table**: `customer_rfm` rows for the selected segment (customer_unique_id,
   recency_days, frequency, monetary) so marketing can export a target list.

Frequency is left out of the heatmap on purpose: in Olist almost everyone buys once,
so F is 1 or 5 and carries little information.

## 9. Check your numbers

The cards should match the pandas implementation of the same measures. From
the repo root on the host:

```powershell
python -c "import pandas as pd; from olistpipe.powerbi import measures; print(measures(pd.read_csv('powerbi/gold_exports/fact_orders.csv')))"
```

GMV, Orders, AOV, On-time Delivery % and Repeat Rate % (unfiltered) must equal the
printed values. If they do not, the usual cause is a relationship pointing the wrong
way or `is_sale` loaded as text.

## 10. Refresh after a new pipeline run

The `export_powerbi` task rewrites `gold_exports/*.csv` on every DAG run (and you can
run it by hand: `python -m olistpipe export-powerbi`). In Power BI Desktop click
**Home > Refresh**. The file paths stay the same, so the model, measures and visuals
keep working.

If you prefer a live connection: **Get data > MySQL database**, server
`localhost:33306`, database `analytics`, user `olist` / `olist` (needs the MySQL
Connector/NET driver). Same tables, same relationships.

## 11. For the portfolio

Take a screenshot of each page (**File > Export > Export to PDF** also works) and put
them next to the matplotlib previews in the README, clearly labelled as the Power BI
version.
