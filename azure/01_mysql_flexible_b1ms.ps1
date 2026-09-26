<#
01_mysql_flexible_b1ms.ps1  -  the "live store" OLTP database in Azure.

What it does
  Creates an Azure Database for MySQL Flexible Server on the Burstable Standard_B1ms
  tier with 32 GB storage, opens the firewall for YOUR current public IP only, creates
  the olist and analytics databases, and runs the same table scripts the local Docker
  MySQL runs (sql/mysql/02_olist_tables.sql and 03_etl_tables.sql).

What it costs
  The Azure free account includes 750 hours/month of B1ms + 32 GB storage + 32 GB
  backup for 12 months. One server running all month is about 730 hours, so ONE server
  fits. Two servers, a bigger SKU, or more storage do not. After 12 months it is billed.
  Stop the server when you are not using it (05_stop_start_mysql.ps1).

Version note: this uses MySQL 8.0.21 because that is the version value every recent
az CLI accepts for flexible servers. Run `az mysql flexible-server list-skus` or check
the portal if you want 8.4; the pipeline works with both (the local container is 8.4).

Needs: the mysql command-line client on PATH (winget install Oracle.MySQL) for the
table scripts, or run them from MySQL Workbench instead.

Usage:
  .\01_mysql_flexible_b1ms.ps1 -AdminPassword "<a strong password>"
#>
param(
    [Parameter(Mandatory = $true)] [string] $AdminPassword,
    [string] $ResourceGroup = "rg-de33",
    [string] $Location = "canadacentral",
    [string] $AdminUser = "olistadmin",
    [string] $ServerName = ("de33-mysql-" + (Get-Random -Maximum 99999))
)
$ErrorActionPreference = "Stop"
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$project = Split-Path -Parent $here

$myIp = (Invoke-RestMethod -Uri "https://api.ipify.org").Trim()
Write-Host "Your public IP is $myIp (only this address will be allowed in)."

Write-Host "Creating MySQL flexible server $ServerName (takes 5-10 minutes)..."
az mysql flexible-server create `
    --resource-group $ResourceGroup --name $ServerName --location $Location `
    --admin-user $AdminUser --admin-password $AdminPassword `
    --tier Burstable --sku-name Standard_B1ms `
    --storage-size 32 --version 8.0.21 `
    --public-access $myIp --backup-retention 7 `
    --high-availability Disabled --storage-auto-grow Disabled -o none

foreach ($db in @("olist", "analytics")) {
    Write-Host "Creating database $db..."
    az mysql flexible-server db create --resource-group $ResourceGroup `
        --server-name $ServerName --database-name $db -o none
}

$hostName = "$ServerName.mysql.database.azure.com"
$ca = Join-Path $here "DigiCertGlobalRootG2.crt.pem"
if (-not (Test-Path $ca)) {
    Write-Host "Downloading the CA certificate Azure MySQL uses for TLS..."
    Invoke-WebRequest -Uri "https://cacerts.digicert.com/DigiCertGlobalRootG2.crt.pem" -OutFile $ca
}

$mysql = Get-Command mysql -ErrorAction SilentlyContinue
if ($mysql) {
    foreach ($f in @("02_olist_tables.sql", "03_etl_tables.sql")) {
        $path = Join-Path $project "sql\mysql\$f"
        Write-Host "Running $f..."
        Get-Content $path -Raw | & mysql --host=$hostName --user=$AdminUser `
            --password=$AdminPassword --ssl-ca=$ca --ssl-mode=VERIFY_CA
    }
} else {
    Write-Host "mysql client not found. Run sql\mysql\02_olist_tables.sql and"
    Write-Host "03_etl_tables.sql yourself (MySQL Workbench works) against $hostName."
}

Write-Host ""
Write-Host "Server:  $hostName"
Write-Host "User:    $AdminUser"
Write-Host "Put these in .env (repo root) as AZURE_MYSQL_HOST / AZURE_MYSQL_USER / AZURE_MYSQL_PASSWORD"
Write-Host "(04_write_env.ps1 does it for you)."
Write-Host "Stop it when idle:  az mysql flexible-server stop -g $ResourceGroup -n $ServerName"
