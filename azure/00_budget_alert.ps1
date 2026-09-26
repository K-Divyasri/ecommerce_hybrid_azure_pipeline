<#
00_budget_alert.ps1  -  RUN THIS FIRST, before creating anything else.

What it does
  1. Logs you in to Azure (browser pop-up) and picks the subscription.
  2. Creates the resource group rg-de33 that every other script uses.
  3. Creates a USD 5 monthly budget on that resource group that emails you at
     80% and 100% of the amount.

What it costs
  Nothing. Resource groups and budgets are free.

Important: a budget is an ALERT, not a spending cap. Azure keeps billing after the
email arrives. The alert gives you time to run 99_teardown.ps1.

Usage (from the azure folder):
  .\00_budget_alert.ps1 -Email you@example.com
  .\00_budget_alert.ps1 -Email you@example.com -Location canadacentral -SubscriptionId <guid>
#>
param(
    [Parameter(Mandatory = $true)] [string] $Email,
    [string] $Location = "canadacentral",
    [string] $ResourceGroup = "rg-de33",
    [string] $SubscriptionId = "",
    [int]    $Amount = 5
)
$ErrorActionPreference = "Stop"

Write-Host "Logging in (a browser window opens)..."
az login | Out-Null
if ($SubscriptionId) { az account set --subscription $SubscriptionId }
$sub = az account show --query id -o tsv
Write-Host "Using subscription $sub"

Write-Host "Creating resource group $ResourceGroup in $Location..."
az group create --name $ResourceGroup --location $Location -o none

# Budget period: first day of the current month, running for one year.
$start = (Get-Date -Day 1).ToString("yyyy-MM-01")
$end   = (Get-Date -Day 1).AddYears(1).ToString("yyyy-MM-01")

# Option A (the one that works reliably): call the Consumption REST API directly with
# az rest. `az consumption budget create` exists but is marked preview, has no flag for
# notification e-mails in most CLI versions, and has been flaky with resource-group
# scope. The REST call below is the documented Microsoft.Consumption/budgets API.
$body = @{
    properties = @{
        category   = "Cost"
        amount     = $Amount
        timeGrain  = "Monthly"
        timePeriod = @{ startDate = "$($start)T00:00:00Z"; endDate = "$($end)T00:00:00Z" }
        notifications = @{
            actual80  = @{ enabled = $true; operator = "GreaterThanOrEqualTo"; threshold = 80;
                           contactEmails = @($Email); thresholdType = "Actual" }
            actual100 = @{ enabled = $true; operator = "GreaterThanOrEqualTo"; threshold = 100;
                           contactEmails = @($Email); thresholdType = "Actual" }
        }
    }
} | ConvertTo-Json -Depth 6 -Compress
$bodyFile = New-TemporaryFile
Set-Content -Path $bodyFile -Value $body -Encoding utf8

$url = "https://management.azure.com/subscriptions/$sub/resourceGroups/$ResourceGroup" +
       "/providers/Microsoft.Consumption/budgets/budget-de33?api-version=2023-05-01"
Write-Host "Creating USD $Amount monthly budget with alerts at 80% and 100% to $Email..."
az rest --method put --url $url --body "@$bodyFile" -o none
Remove-Item $bodyFile

# Option B (kept for reference, no e-mail notifications):
# az consumption budget create --budget-name budget-de33 --amount $Amount `
#   --category cost --time-grain monthly --start-date $start --end-date $end `
#   --resource-group $ResourceGroup

Write-Host ""
Write-Host "Done. Check it in the portal: Cost Management > Budgets."
Write-Host "Next: .\01_mysql_flexible_b1ms.ps1"
