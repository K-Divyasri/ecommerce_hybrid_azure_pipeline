<#
02_storage_account.ps1  -  Azure Blob Storage, the cloud landing/backup zone.

What it does
  Creates a StorageV2 account (Standard_LRS, Hot tier) and a private container named
  "landing". The extract step copies every raw Parquet file here, the same thing it
  does against Azurite locally.

What it costs
  The free account includes 5 GB of LRS Hot blob storage plus a generous number of
  read/write operations for 12 months. This project writes a few MB per day. LRS
  (three copies in one datacenter) is the cheapest redundancy; do not pick GRS.

Usage:
  .\02_storage_account.ps1
#>
param(
    [string] $ResourceGroup = "rg-de33",
    [string] $Location = "canadacentral",
    # storage account names: 3-24 chars, lowercase letters and digits only, globally unique
    [string] $AccountName = ("de33lake" + (Get-Random -Maximum 999999))
)
$ErrorActionPreference = "Stop"

Write-Host "Creating storage account $AccountName..."
az storage account create --name $AccountName --resource-group $ResourceGroup `
    --location $Location --sku Standard_LRS --kind StorageV2 --access-tier Hot `
    --min-tls-version TLS1_2 --allow-blob-public-access false -o none

Write-Host "Creating private container 'landing'..."
az storage container create --name landing --account-name $AccountName --auth-mode login -o none 2>$null
if ($LASTEXITCODE -ne 0) {
    # --auth-mode login needs the "Storage Blob Data Contributor" role; fall back to the key
    $key = az storage account keys list -g $ResourceGroup -n $AccountName --query "[0].value" -o tsv
    az storage container create --name landing --account-name $AccountName --account-key $key -o none
}

$cs = az storage account show-connection-string -g $ResourceGroup -n $AccountName --query connectionString -o tsv
$masked = $cs -replace "AccountKey=[^;]+", "AccountKey=****"
Write-Host ""
Write-Host "Account: $AccountName"
Write-Host "Connection string (masked): $masked"
Write-Host "04_write_env.ps1 copies the real value into .env (repo root) as AZURE_STORAGE_CONNECTION_STRING."
