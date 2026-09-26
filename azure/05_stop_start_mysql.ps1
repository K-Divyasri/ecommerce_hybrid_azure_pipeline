<#
05_stop_start_mysql.ps1  -  stop the MySQL server when you are not studying.

What it does
  Stops or starts the flexible server in rg-de33. A stopped server does not use
  compute hours (storage is still counted, and is inside the 32 GB free allowance).

Catch: Azure automatically STARTS a stopped flexible server after 30 days. If you
leave for a month, stop it again, or run 99_teardown.ps1.

What it costs
  Nothing by itself; it saves your 750 free hours.

Usage:
  .\05_stop_start_mysql.ps1 stop
  .\05_stop_start_mysql.ps1 start
  .\05_stop_start_mysql.ps1 status
#>
param(
    [Parameter(Mandatory = $true)] [ValidateSet("stop", "start", "status")] [string] $Action,
    [string] $ResourceGroup = "rg-de33"
)
$ErrorActionPreference = "Stop"
$name = az mysql flexible-server list -g $ResourceGroup --query "[0].name" -o tsv
if (-not $name) { Write-Host "No MySQL flexible server in $ResourceGroup."; exit 1 }

switch ($Action) {
    "stop"   { az mysql flexible-server stop  -g $ResourceGroup -n $name -o none; Write-Host "Stopped $name." }
    "start"  { az mysql flexible-server start -g $ResourceGroup -n $name -o none; Write-Host "Started $name." }
    "status" { az mysql flexible-server show  -g $ResourceGroup -n $name --query state -o tsv }
}
