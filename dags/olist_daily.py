"""
Daily e-commerce pipeline (Airflow 3).

    extract_table x8 (mapped)  ->  spark_gold  ->  rfm_and_anomalies  ->  cosmos_customer360
                                                                      ->  export_powerbi

The DAG is deliberately thin: every task is one call into the olistpipe package, which
is tested without Airflow. Airflow only decides WHEN and IN WHAT ORDER.

Run one day by hand inside the scheduler container:
    airflow dags test olist_daily 2018-09-01
The run date is the logical date (ds) unless you pass params {"run_date": "YYYY-MM-DD"}.
"""
from __future__ import annotations

import sys
from datetime import date, datetime, timedelta

from airflow.sdk import dag, get_current_context, task

sys.path.insert(0, "/opt/project")  # the project folder is mounted here by docker compose

from olistpipe.schema import TABLE_NAMES  # noqa: E402


def _run_date() -> date:
    ctx = get_current_context()
    override = (ctx.get("params") or {}).get("run_date")
    return date.fromisoformat(override or ctx["ds"])


@dag(
    dag_id="olist_daily",
    schedule="@daily",
    start_date=datetime(2018, 8, 1),  # must be <= the first run date or no tasks get created
    catchup=False,
    max_active_runs=1,  # watermarks move forward one day at a time
    params={"run_date": ""},
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["olist", "de33"],
)
def olist_daily():

    @task(max_active_tis_per_dagrun=4)
    def extract_table(table: str) -> dict:
        """Incremental pull of one table: MySQL -> Parquet -> HDFS raw + Blob landing."""
        from olistpipe.pipeline import step_extract
        w = step_extract(_run_date(), tables=[table])[0]
        return {"table": w.table, "rows": w.row_count, "rerun": w.rerun}

    @task(retries=0)
    def spark_gold(extracted: list) -> None:
        """Silver, quality gate, star schema, serving tables. Exit code 2 = gate failed:
        fail fast with no retry, because rerunning the same bad data fails the same way."""
        from airflow.exceptions import AirflowFailException
        from olistpipe.pipeline import step_spark
        print("extracted:", extracted)
        rc = step_spark(_run_date(), jdbc=True)
        if rc == 2:
            raise AirflowFailException("data quality gate failed; see quarantine tables in HDFS")
        if rc != 0:
            raise RuntimeError(f"spark job exited with {rc}")

    @task
    def rfm_and_anomalies() -> dict:
        from olistpipe.pipeline import step_analytics
        return step_analytics(_run_date())

    @task
    def cosmos_customer360() -> dict:
        from olistpipe.pipeline import step_cosmos
        return step_cosmos(_run_date())

    @task
    def export_powerbi() -> dict:
        from olistpipe.pipeline import step_export_powerbi
        return step_export_powerbi()

    extracted = extract_table.expand(table=TABLE_NAMES)
    analytics = rfm_and_anomalies()
    spark_gold(extracted) >> analytics
    analytics >> [cosmos_customer360(), export_powerbi()]


olist_daily()
