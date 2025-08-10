# ----- EDIT THESE PATHS/VALUES -----
$IndexerProj = 'C:\Users\denni\OneDrive\B6\backend\B6.Indexer\B6.Indexer.csproj'
$PublishDir  = 'C:\B6Missions\Indexer'

$ServiceName = 'B6Indexer'
$DisplayName = 'B6 Indexer'
$Description = 'Background blockchain indexer for B6 missions'

# Runtime config used by the indexer:
$Rpc     = 'https://evm.cronos.org'
$Factory = '0xb148389C2c554398d5D96B4E795945F85cf80801'
$DbConn  = 'Host=localhost;Database=b6_game;Username=postgres;Password=@PTeaowbewhyos01;Pooling=true;'

# -----------------------------------

$ErrorActionPreference = 'Stop'

# Must be admin
if (-not ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()
  ).IsInRole([Security.Principal.WindowsBuiltinRole]::Administrator)) {
  throw "Run PowerShell as Administrator."
}

Write-Host "Publishing B6.Indexer..." -ForegroundColor Cyan
dotnet publish $IndexerProj -c Release -r win-x64 --self-contained `
  -p:PublishSingleFile=false -o $PublishDir

New-Item -ItemType Directory -Path 'c:\B^missions\Indexer' -Force | Out-Null

# Write appsettings.Production.json next to the exe
$appSettings = Join-Path $PublishDir 'appsettings.Production.json'
@"
{
  "Cronos": { "Rpc": "$Rpc" },
  "Contracts": { "Factory": "$Factory" },
  "ConnectionStrings": { "Db": "$DbConn" },
  "Logging": { "LogLevel": { "Default": "Information", "Microsoft.Hosting.Lifetime": "Information" } }
}
"@ | Out-File -FilePath $appSettings -Encoding UTF8 -Force

# (Re)create Windows Service
$exe = Join-Path $PublishDir 'B6.Indexer.exe'

Write-Host "Configuring Windows Service '$ServiceName'..." -ForegroundColor Cyan
$svc = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if ($svc) {
  try { Stop-Service $ServiceName -Force -ErrorAction SilentlyContinue } catch {}
  sc.exe delete "$ServiceName" | Out-Null
  Start-Sleep -Seconds 2
}

# Create as LocalSystem; set auto-start + delayed auto start
sc.exe create "$ServiceName" binPath= "`"$exe`"" start= auto obj= "LocalSystem" displayname= "$DisplayName" | Out-Null
Set-ItemProperty "HKLM:\SYSTEM\CurrentControlSet\Services\$ServiceName" -Name Description -Value $Description
Set-ItemProperty "HKLM:\SYSTEM\CurrentControlSet\Services\$ServiceName" -Name DelayedAutoStart -Value 1

# Service recovery: restart on failure
sc.exe failureflag $ServiceName 1 | Out-Null
sc.exe failure $ServiceName reset= 86400 actions= restart/6000/restart/60000/restart/60000 | Out-Null

# Start it
Write-Host "Starting service..." -ForegroundColor Cyan
Start-Service $ServiceName
Start-Sleep 2

Get-Service $ServiceName | Format-Table -Auto
Write-Host "Done. Service should now run independently of IIS." -ForegroundColor Green
