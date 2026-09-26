<#
04_write_env.ps1  -  point the pipeline at real Azure.

What it does
  Looks up the MySQL server, storage account and Cosmos account in rg-de33 and writes
  .env (repo root) with RUN_MODE=azure plus the connection details. Secrets go into the
  file only (it is git-ignored); the console shows masked values.
  An existing .env is backed up to .env.bak first.

What it costs
  Nothing (read-only az calls).

Usage:
  .\04_write_env.ps1 -MySqlPassword "<the password you used in script 01>"
Then restart the stack so containers pick up the new values:
  docker compose up -d --force-recreate
#>
param(
    [Parameter(Mandatory = $true)] [string] $MySqlPassword,
    [string] $ResourceGroup = "rg-de33"
)
$ErrorActionPreference = "Stop"
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$project = Split-Path -Parent $here
$envFile = Join-Path $project ".env"

function Mask([string] $s) { if ($s.Length -le 6) { "****" } else { $s.Substring(0, 4) + "****" } }

$mysqlName = az mysql flexible-server list -g $ResourceGroup --query "[0].name" -o tsv
$mysqlUser = az mysql flexible-server show -g $ResourceGroup -n $mysqlName --query administratorLogin -o tsv
$storage = az storage account list -g $ResourceGroup --query "[0].name" -o tsv
$blobCs = az storage account show-connection-string -g $ResourceGroup -n $storage --query connectionString -o tsv
$cosmos = az cosmosdb list -g $ResourceGroup --query "[0].name" -o tsv
$cosmosEndpoint = az cosmosdb show -n $cosmos -g $ResourceGroup --query documentEndpoint -o tsv
$cosmosKey = az cosmosdb keys list -n $cosmos -g $ResourceGroup --query primaryMasterKey -o tsv

if (Test-Path $envFile) { Copy-Item $envFile "$envFile.bak" -Force }

# The CA path is the path INSIDE the containers (project is mounted at /opt/project).
# When you run olistpipe from the Windows host instead, config.py falls back to
# azure\DigiCertGlobalRootG2.crt.pem if you delete this line.
$lines = @(
    "RUN_MODE=azure",
    "AIRFLOW_UID=50000",
    "AZURE_MYSQL_HOST=$mysqlName.mysql.database.azure.com",
    "AZURE_MYSQL_PORT=3306",
    "AZURE_MYSQL_USER=$mysqlUser",
    "AZURE_MYSQL_PASSWORD=$MySqlPassword",
    "AZURE_MYSQL_SSL_CA=/opt/project/azure/DigiCertGlobalRootG2.crt.pem",
    "AZURE_STORAGE_CONNECTION_STRING=$blobCs",
    "AZURE_COSMOS_ENDPOINT=$cosmosEndpoint",
    "AZURE_COSMOS_KEY=$cosmosKey"
)
Set-Content -Path $envFile -Value $lines -Encoding ascii

Write-Host "Wrote $envFile"
Write-Host "  AZURE_MYSQL_HOST      = $mysqlName.mysql.database.azure.com"
Write-Host "  AZURE_MYSQL_USER      = $mysqlUser"
Write-Host "  AZURE_MYSQL_PASSWORD  = $(Mask $MySqlPassword)"
Write-Host "  AZURE_STORAGE_...     = account $storage, key $(Mask ($blobCs -replace '.*AccountKey=([^;]+).*','$1'))"
Write-Host "  AZURE_COSMOS_ENDPOINT = $cosmosEndpoint"
Write-Host "  AZURE_COSMOS_KEY      = $(Mask $cosmosKey)"
Write-Host ""
Write-Host "Restart the stack: docker compose up -d --force-recreate"
