<#
99_teardown.ps1  -  delete everything this project created in Azure.

What it does
  Deletes the whole resource group rg-de33: the MySQL server, the storage account
  (and every blob in it), the Cosmos DB account, and the budget scoped to the group.
  This cannot be undone.

What it costs
  Nothing; it stops all charges from these resources once deletion finishes
  (it can take 10+ minutes; the command returns before that).

Usage:
  .\99_teardown.ps1
#>
param([string] $ResourceGroup = "rg-de33")
$ErrorActionPreference = "Stop"

Write-Host "This PERMANENTLY deletes resource group '$ResourceGroup' and everything in it:"
az resource list -g $ResourceGroup --query "[].{name:name, type:type}" -o table
$answer = Read-Host "Type the resource group name to confirm"
if ($answer -ne $ResourceGroup) { Write-Host "Not confirmed. Nothing deleted."; exit 1 }

az group delete --name $ResourceGroup --yes --no-wait
Write-Host "Deletion started."
Write-Host "Afterwards:"
Write-Host "  - portal.azure.com > Resource groups: rg-de33 should disappear within ~15 minutes"
Write-Host "  - Cost Management > Cost analysis: check nothing else is still billing"
Write-Host "  - the budget was scoped to the group and goes with it; recreate it with"
Write-Host "    00_budget_alert.ps1 if you build again"
Write-Host "  - set RUN_MODE=local in .env (or restore .env.bak)"
