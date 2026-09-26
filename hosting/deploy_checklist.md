# Deploy checklist

## Before pushing to GitHub
- [ ] `git status` shows no `.env`, no `data/raw/`, no `*.pem`, no large Parquet
- [ ] Real Olist CSVs (if you used them) are NOT staged (CC BY-NC-SA 4.0)
- [ ] `python -m pytest` passes at the repo root (host tests)
- [ ] `docker compose run --rm spark python -m pytest tests/spark` passes
- [ ] `python -m olistpipe ddl` matches `sql/mysql/02_olist_tables.sql`

## GitHub
- [ ] Repo created, code pushed to `main`
- [ ] `.github/workflows/ci.yml` and `hosting/github_actions/ci.yml` identical
- [ ] First CI run green (both jobs)
- [ ] CI badge added to README

## Local full-stack run
- [ ] `docker compose up -d --build` and every service shows `healthy`
- [ ] `python -m olistpipe generate` and `seed-mysql` done
- [ ] `airflow dags test olist_daily 2018-08-31` succeeded
- [ ] Simulated a week, ran the next date, reran it: same counts, 0 Cosmos upserts
- [ ] Power BI exports and previews regenerated in `powerbi/`
- [ ] Screenshots taken (Airflow grid, HDFS browser, Cosmos explorer)
- [ ] `docker compose down -v` when finished

## Real Azure (optional)
- [ ] `00_budget_alert.ps1` run FIRST and the budget visible in Cost Management
- [ ] MySQL B1ms, storage account, Cosmos free tier created
- [ ] `04_write_env.ps1` wrote `.env` with `RUN_MODE=azure`
- [ ] DigiCertGlobalRootG2.crt.pem in `azure/`
- [ ] Pipeline ran end to end against Azure
- [ ] MySQL server stopped when idle (`05_stop_start_mysql.ps1 stop`)
- [ ] `99_teardown.ps1` run, resource group gone, `.env` back to `RUN_MODE=local`

## Power BI
- [ ] Report built in Power BI Desktop following `powerbi/BUILD_GUIDE.md`
- [ ] Theme imported, all measures from `measures.dax` added
- [ ] Three pages screenshotted for the README / portfolio
