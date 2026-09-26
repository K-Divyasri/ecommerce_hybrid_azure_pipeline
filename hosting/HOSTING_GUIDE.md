# Hosting guide

Short version: there is no website to host. This project is a data pipeline. Its
"users" are a BI report, a customer-service tool reading Cosmos DB, and analysts
querying MySQL. What you publish is the code (GitHub), proof that it works (CI), and
optionally a run against real Azure free-tier services.

## 1. Put it on GitHub

The repo root holds the pipeline directly (`olistpipe/`, `spark_jobs/`, `dags/`,
`docker-compose.yml`, ...).

```powershell
git add .
git status        # check: no .env, no data/raw, no *.pem, no big Parquet files
git commit -m "E-commerce hybrid on-prem/Azure pipeline"
git push -u origin main
```

Before every push, run `git status` and read it. `.gitignore` keeps out `.env`,
generated data and the real Olist CSVs (CC BY-NC-SA 4.0: you may not redistribute
them inside your repo). The small sample in `data/sample/olist/` is generated
by this project and is fine to commit.

## 2. Turn on CI (GitHub Actions, free for public repos)

GitHub only runs workflows from `.github/workflows/` at the repo root. The live
workflow is `.github/workflows/ci.yml`; `hosting/github_actions/ci.yml` is an identical
reference copy. If you change one, copy it over the other:

```powershell
Copy-Item hosting\github_actions\ci.yml .github\workflows\ci.yml
```

What CI proves on every push, with **zero secrets and zero services**:

| Job | What it runs | What it proves |
|-----|--------------|----------------|
| `python-tests` | pytest on Python 3.12 with SQLite and the in-memory Cosmos fake | generator is deterministic, incremental extract is idempotent (rerun = same file, no duplicates), keyset pagination with the row_id tie-break, RFM scoring, z-score anomalies, customer 360 docs, Power BI measure definitions |
| `python-tests` | `python -m olistpipe ddl` diffed against `sql/mysql/02_olist_tables.sql` | the MySQL schema file has not drifted from the Python schema |
| `spark-tests` | pytest on a local SparkSession (runner has Java 17) | row_number dedupe, no join fan-out, star schema grain, untranslated category fallback, data quality quarantine and gate |

What CI does **not** prove: that HDFS, Airflow, MySQL, Azurite and the Cosmos emulator
wire together. That needs the Docker stack (section 3). Say this plainly in interviews;
it is the normal split between unit tests and integration runs.

Add a badge to the README once the first run is green:

```markdown
![ci](https://github.com/<you>/ecommerce-hybrid-azure-pipeline/actions/workflows/ci.yml/badge.svg)
```

## 3. Run the full stack locally (Docker, $0)

This is the main way to run the project; see the quickstart in the top-level
`README.md`. Everything (MySQL, HDFS namenode + datanode, Azurite, Cosmos emulator,
Airflow, Spark) runs on your laptop. Plan for about 6 GB of RAM for Docker.

```powershell
Copy-Item .env.example .env
docker compose up -d --build
```

Ports: Airflow 33080, HDFS UI 33870, MySQL 33306, Azurite 33000, Cosmos 33081
(explorer 33234), Spark UI 33040.

## 4. Run the cloud half on real Azure (free tier)

Follow `azure/README.md`. In order: budget alert first, then MySQL Flexible
Server B1ms, a storage account, Cosmos DB free tier, then `04_write_env.ps1` flips
`.env` to `RUN_MODE=azure`. HDFS, Spark and Airflow stay in Docker because the managed
Azure versions (HDInsight, Synapse, Databricks, managed Airflow) have no free tier.

Tear it down with `99_teardown.ps1` when you have your screenshots.

## 5. What to show a hiring manager

- The GitHub repo with a green CI badge.
- Screenshots: the Airflow grid view of `olist_daily` succeeding, the HDFS browser
  (localhost:33870) showing `ingest_date=` partitions, the Cosmos data explorer
  (localhost:33234) with a customer 360 document, and your Power BI Desktop pages built
  from `powerbi/BUILD_GUIDE.md`.
- The idempotency proof: rerunning the same day gives identical row
  counts and zero Cosmos upserts.

See `deploy_checklist.md` for a tick-box version.
