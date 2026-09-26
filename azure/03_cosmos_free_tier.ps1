<#
03_cosmos_free_tier.ps1  -  Cosmos DB for NoSQL with the free tier discount.

What it does
  Creates a Cosmos DB account (NoSQL API) with --enable-free-tier true, a database
  "olist" with 1000 RU/s of SHARED (database-level) throughput, and two containers
  inside it:
     customer360   partition key /customer_unique_id
     reviews       partition key /order_id

What it costs
  Free tier gives the first 1000 RU/s and 25 GB of storage free, forever, but only ONE
  free-tier account per subscription. If the flag is refused, you already have one
  somewhere: delete it or reuse it. Throughput above 1000 RU/s is billed, which is why
  the two containers SHARE the database's 1000 RU/s instead of each getting their own
  400 RU/s minimum (2 x 400 = 800 would also fit, but a third container would not,
  and shared throughput keeps you safely under the line no matter how many you add).

Serverless accounts cannot use free tier; this script uses provisioned throughput.

Usage:
  .\03_cosmos_free_tier.ps1
#>
param(
    [string] $ResourceGroup = "rg-de33",
    [string] $Location = "canadacentral",
    [string] $AccountName = ("de33-cosmos-" + (Get-Random -Maximum 99999))
)
$ErrorActionPreference = "Stop"

Write-Host "Creating Cosmos DB account $AccountName with free tier (takes 5-10 minutes)..."
az cosmosdb create --name $AccountName --resource-group $ResourceGroup `
    --locations regionName=$Location failoverPriority=0 isZoneRedundant=False `
    --enable-free-tier true --default-consistency-level Session -o none

Write-Host "Creating database 'olist' with 1000 RU/s shared throughput..."
az cosmosdb sql database create --account-name $AccountName --resource-group $ResourceGroup `
    --name olist --throughput 1000 -o none

Write-Host "Creating container customer360 (pk /customer_unique_id)..."
az cosmosdb sql container create --account-name $AccountName --resource-group $ResourceGroup `
    --database-name olist --name customer360 --partition-key-path "/customer_unique_id" -o none

Write-Host "Creating container reviews (pk /order_id)..."
az cosmosdb sql container create --account-name $AccountName --resource-group $ResourceGroup `
    --database-name olist --name reviews --partition-key-path "/order_id" -o none

$endpoint = az cosmosdb show -n $AccountName -g $ResourceGroup --query documentEndpoint -o tsv
$key = az cosmosdb keys list -n $AccountName -g $ResourceGroup --query primaryMasterKey -o tsv
Write-Host ""
Write-Host "Endpoint: $endpoint"
Write-Host ("Key:      " + $key.Substring(0, 4) + "****  (04_write_env.ps1 writes the full key to .env)")
