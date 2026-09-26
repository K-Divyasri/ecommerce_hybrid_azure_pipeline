# Running the cloud half on real Azure (free tier)

Everything in this project runs locally with emulators by default (`RUN_MODE=local`).
These scripts are for when you want the same pipeline talking to real Azure services:
Azure Database for MySQL Flexible Server instead of the MySQL container, a real storage
account instead of Azurite, and Cosmos DB free tier instead of the emulator.

HDFS, Spark and Airflow stay in Docker on your laptop either way. That is the "hybrid"
in the project name: the on-prem side (Hadoop cluster, Spark jobs, scheduler) pushes to
and reads from managed cloud services over the internet.

Nothing here runs automatically. You run each script yourself, read what it prints, and
you can stop after any step.

## Before you start: the catches

- **A credit card is required** to create an Azure free account, even though nothing
  here should cost money. Microsoft uses it for identity checks.
- **The free account has two clocks.** You get USD 200 credit for 30 days, and a list
  of services free for 12 months. After day 30 the 12-month services keep working only
  if you **upgrade to pay-as-you-go**. Upgrading does not start billing by itself: you
  still pay nothing while you stay inside the free limits. If you do not upgrade, the
  resources are disabled after 30 days.
- **Stop MySQL when you are not using it.** B1ms is free for 750 hours a month, which
  is one server running all month. A stopped server uses no hours. Azure automatically
  **starts** a stopped flexible server again after 30 days, so a forgotten server comes
  back on its own. `05_stop_start_mysql.ps1 stop` is your friend.
- **Cosmos DB free tier is one account per subscription.** 1000 RU/s and 25 GB free
  forever. A second free-tier account is refused.
- **Why Spark, Hadoop and Airflow are not in Azure here:** Azure Databricks, Synapse
  Spark pools, HDInsight and Data Factory's managed Airflow have **no free tier**. They
  bill by the hour from the moment they exist. Running them in Docker on your laptop is
  the honest $0 option, and the code is the same code a cluster would run.
- **Free limits change.** Check [azure.microsoft.com/free](https://azure.microsoft.com/free)
  before you start. The numbers in these scripts were correct when written.
- A **budget is an alert, not a cap.** It emails you; it does not stop spending.

## Order to run

Open PowerShell in `azure\` and install the Azure CLI first
(`winget install Microsoft.AzureCLI`, then open a new terminal).

| Step | Script | Creates | Free-tier notes |
|------|--------|---------|-----------------|
| 0 | `00_budget_alert.ps1 -Email you@example.com` | resource group `rg-de33`, USD 5 budget with 80%/100% email alerts | free |
| 1 | `01_mysql_flexible_b1ms.ps1 -AdminPassword "..."` | MySQL Flexible Server, Burstable B1ms, 32 GB, databases `olist` + `analytics`, tables | 750 h/month + 32 GB for 12 months |
| 2 | `02_storage_account.ps1` | StorageV2, Standard_LRS, Hot, container `landing` | 5 GB LRS Hot for 12 months |
| 3 | `03_cosmos_free_tier.ps1` | Cosmos DB NoSQL (free tier), database `olist` at 1000 RU/s shared, containers `customer360`, `reviews` | 1000 RU/s + 25 GB, forever, one per subscription |
| 4 | `04_write_env.ps1 -MySqlPassword "..."` | writes `.env` with `RUN_MODE=azure` | free |
| - | `05_stop_start_mysql.ps1 stop` | stops the MySQL server | saves hours |
| 99 | `99_teardown.ps1` | deletes `rg-de33` and everything in it | stops all charges |

Script 00 must come first so the alert exists before anything that could cost money.

## Filling in .env by hand

`04_write_env.ps1` does this for you. If you prefer to do it yourself, copy
`.env.example` to `.env` and fill in the same names:

```
RUN_MODE=azure
AZURE_MYSQL_HOST=<server>.mysql.database.azure.com
AZURE_MYSQL_USER=olistadmin
AZURE_MYSQL_PASSWORD=<your password>
AZURE_MYSQL_SSL_CA=/opt/project/azure/DigiCertGlobalRootG2.crt.pem
AZURE_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net
AZURE_COSMOS_ENDPOINT=https://<account>.documents.azure.com:443/
AZURE_COSMOS_KEY=<primary key>
```

`.env` is git-ignored. Never commit it, and never paste the keys into any file you
might push.

## The TLS certificate (DigiCertGlobalRootG2.crt.pem)

Azure MySQL only accepts encrypted connections. To verify it is really talking to
Azure (and not someone in the middle), the client needs the certificate authority that
signed the server's certificate. Script 01 downloads it into this folder:

```powershell
Invoke-WebRequest https://cacerts.digicert.com/DigiCertGlobalRootG2.crt.pem -OutFile DigiCertGlobalRootG2.crt.pem
```

`config.py` passes it to PyMySQL (`ssl_ca=`) and the Spark JDBC URL switches to
`useSSL=true`. Inside the containers the path is `/opt/project/azure/...` because the
project folder is mounted at `/opt/project`. The certificate is public, but keep it out
of git anyway (it is listed in `.gitignore`) so everyone downloads a current copy.

## Firewall

Script 01 allows only your current public IP. Home IPs change; if connections start
timing out, add the new one:

```powershell
$ip = (Invoke-RestMethod https://api.ipify.org).Trim()
az mysql flexible-server firewall-rule create -g rg-de33 -n <server> --rule-name home2 --start-ip-address $ip --end-ip-address $ip
```

The storage account and Cosmos DB accept connections from anywhere with the key by
default. That is fine for a demo project; in a real company you would restrict
them to private endpoints or selected networks.

## How the Docker side reaches Azure

Nothing special: the Airflow containers read the same `.env` through
`env_file` in `docker-compose.yml` and connect out over the internet, exactly like your
laptop's browser does. After changing `.env`:

```powershell
cd ..
docker compose up -d --force-recreate
docker compose exec airflow-scheduler airflow dags test olist_daily 2018-08-31
```

Seed Azure MySQL from the host the same way as locally
(`python -m olistpipe seed-mysql` with `RUN_MODE=azure` in `.env`). With a few
thousand orders the whole run stays far inside every free limit.

## When you are done

```powershell
.\99_teardown.ps1
```

Then check the portal (Resource groups and Cost Management) a day later.
