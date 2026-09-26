"""
The Spark job: HDFS raw -> silver -> quality gate -> gold (HDFS) -> MySQL serving tables.

Run inside Docker (Spark needs Java; on bare Windows it also needs winutils.exe, so this
project never runs Spark on the host):

    # inside the Airflow container (the DAG does this for you)
    python -m spark_jobs.build_gold --run-date 2018-09-02 --jdbc

    # against a local raw folder, no HDFS or MySQL (quick local experiments)
    python -m spark_jobs.build_gold --run-date 2018-08-31 --raw-root file:///work/raw --out-root file:///work/out

Exit codes: 0 ok, 2 data quality gate failed (gold not written), 1 anything else.
"""
from __future__ import annotations

import argparse
import os
import sys
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import dq
from .transforms import NATURAL_KEYS, build_gold, to_silver

MYSQL_JAR = "/opt/jars/mysql-connector-j.jar"

# Serving tables written to MySQL (analytics schema). Everything goes to HDFS gold.
SERVING = ["dim_date", "dim_customer", "dim_product", "dim_seller", "fact_orders",
           "fact_order_items", "fact_reviews", "agg_delivery_by_state",
           "agg_seller_scorecard", "agg_monthly_gmv"]


def get_spark(app: str = "olist-gold", shuffle_partitions: int = 8) -> SparkSession:
    """TUNING NOTE: spark.sql.shuffle.partitions defaults to 200. Every groupBy/join then
    produces 200 tasks; with a few MB of data that is 200 near-empty tasks, each with
    scheduling overhead, and 200 tiny output files. 8 matches a laptop's cores. Adaptive
    Query Execution (on by default since Spark 3.2) can also coalesce small partitions at
    runtime; setting a sane starting value still helps. On a real cluster with 100 GB,
    you would go the other way (hundreds to thousands)."""
    b = (SparkSession.builder.appName(app)
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.session.timeZone", "UTC")
         # one datanode: tell the HDFS client to write 1 copy, not the default 3
         .config("spark.hadoop.dfs.replication", "1")
         .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
         .config("spark.ui.port", os.environ.get("SPARK_UI_PORT", "4040"))
         .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "1g")))
    if os.path.exists(MYSQL_JAR):
        b = b.config("spark.jars", MYSQL_JAR)
    return b.getOrCreate()


def read_raw(spark: SparkSession, raw_root: str, run_date: str) -> dict[str, DataFrame]:
    """Every ingest_date partition up to and including run_date. Filtering on the
    partition column means Spark lists only those folders (partition pruning), and a
    backfill for an old date never sees files from the future."""
    out = {}
    for table in NATURAL_KEYS:
        path = f"{raw_root.rstrip('/')}/{table}"
        df = spark.read.option("basePath", path).parquet(path)
        out[table] = df.filter(F.col("ingest_date") <= F.lit(run_date))
    return out


def write_parquet(df: DataFrame, path: str, rows: int | None = None) -> None:
    """Small-files guard: 27 rows spread over 8 files (one per shuffle partition) is
    the classic "small files problem" that slows every later read and bloats the
    namenode's memory. When the row count is known, aim for ~1M rows per file."""
    if rows is not None:
        df = df.coalesce(max(1, -(-rows // 1_000_000)))
    df.write.mode("overwrite").parquet(path)


def write_mysql(df: DataFrame, table: str, jdbc_url: str, user: str, password: str) -> None:
    """mode=overwrite + truncate=true: empty the table and refill it, but keep the
    table definition (and any indexes/grants) instead of dropping and recreating.

    Two type fixes on the way out: Spark maps BooleanType to MySQL BIT(1), which Python
    drivers return as bytes (b''), so booleans go out as TINYINT 0/1. And strings
    would become LONGTEXT, which MySQL cannot index, so they get VARCHAR sizes."""
    from pyspark.sql.types import BooleanType, StringType
    for f in df.schema.fields:
        if isinstance(f.dataType, BooleanType):
            df = df.withColumn(f.name, F.col(f.name).cast("tinyint"))
    col_types = ", ".join(
        f"{f.name} VARCHAR({1000 if 'comment' in f.name else 255})"
        for f in df.schema.fields if isinstance(f.dataType, StringType))
    writer = df.write.format("jdbc")
    if col_types:
        writer = writer.option("createTableColumnTypes", col_types)
    (writer
       .option("url", jdbc_url).option("dbtable", table)
       .option("user", user).option("password", password)
       .option("driver", "com.mysql.cj.jdbc.Driver")
       .option("truncate", "true").option("batchsize", "5000")
       .mode("overwrite").save())


def run(spark: SparkSession, raw_root: str, out_root: str, run_date: str,
        max_bad_ratio: float, jdbc: dict | None = None, log=print) -> dict:
    t0 = time.time()
    raw = read_raw(spark, raw_root, run_date)
    raw_counts = {t: df.count() for t, df in raw.items()}
    silver = to_silver(raw)
    good, bad, results = dq.run_checks(silver)

    out = out_root.rstrip("/")
    for table, df in bad.items():  # quarantine is written even when the gate fails
        write_parquet(df, f"{out}/quarantine/{table}/run_date={run_date}", rows=0)
    dq.gate(results, max_bad_ratio, log=log)  # raises DataQualityError -> exit 2

    # cache: each silver table feeds several gold tables; without cache() Spark would
    # re-read and re-deduplicate the raw files once per downstream use
    good = {t: df.cache() for t, df in good.items()}
    for table, df in good.items():
        write_parquet(df, f"{out}/silver/{table}", rows=df.count())
    gold = build_gold(spark, good)
    counts = {}
    for name, df in gold.items():
        df = df.cache()
        counts[name] = df.count()
        write_parquet(df, f"{out}/gold/{name}", rows=counts[name])
        log(f"[gold] {name:24s} rows={counts[name]}")
    if jdbc:
        for name in SERVING:
            write_mysql(gold[name], name, jdbc["url"], jdbc["user"], jdbc["password"])
        log(f"[serve] wrote {len(SERVING)} tables to MySQL analytics schema")
    return {"raw_rows": raw_counts, "gold_rows": counts,
            "quarantined": {r.table: r.bad for r in results},
            "seconds": round(time.time() - t0, 1)}


def explain_broadcast(spark: SparkSession, gold_root: str) -> str:
    """Show the physical plan of a fact-to-dimension join. Look for BroadcastHashJoin
    (small side copied to every task) instead of SortMergeJoin (both sides shuffled)."""
    fi = spark.read.parquet(f"{gold_root}/fact_order_items")
    dp = spark.read.parquet(f"{gold_root}/dim_product")
    j = fi.join(F.broadcast(dp), "product_id").groupBy("category").agg(F.sum("item_value"))
    return j._jdf.queryExecution().executedPlan().toString()


def main(argv=None) -> int:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from olistpipe.config import get_settings
    s = get_settings()
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-date", required=True)
    ap.add_argument("--raw-root", default=f"{s.hdfs_uri}{s.raw_root}")
    ap.add_argument("--out-root", default=f"{s.hdfs_uri}/olist")
    ap.add_argument("--max-bad-ratio", type=float, default=s.dq_max_bad_ratio)
    ap.add_argument("--jdbc", action="store_true", help="also write serving tables to MySQL")
    args = ap.parse_args(argv)

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    jdbc = ({"url": s.jdbc_url, "user": s.mysql_user, "password": s.mysql_password}
            if args.jdbc else None)
    try:
        summary = run(spark, args.raw_root, args.out_root, args.run_date, args.max_bad_ratio, jdbc)
        print(f"[spark] done in {summary['seconds']}s")
        print("[spark] plan for fact_order_items JOIN broadcast(dim_product):")
        plan = explain_broadcast(spark, f"{args.out_root.rstrip('/')}/gold")
        print("\n".join(line for line in plan.splitlines() if "Join" in line or "Exchange" in line))
        return 0
    except dq.DataQualityError as e:
        print(f"[dq] GATE FAILED: {e}")
        return 2
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
