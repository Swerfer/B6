# --- EDIT THESE ---
$Server       = '172.201.105.90'
$User         = 'B6Missions\dennis'               # admin on the server
$Project      = 'C:\Users\denni\OneDrive\B6\backend\B6.Indexer'
$LocalOut     = 'C:\_publish\B6.Indexer'          # local temp output on YOUR PC
$RemoteDir    = 'C:\B6Missions\Indexer'           # target folder on the SERVER
$ServiceName  = 'B6Indexer'
# ------------------

$ErrorActionPreference = 'Stop'

# 1) Publish locally
dotnet publish $Project -c Release -o $LocalOut

# 2) Make a clean zip of the publish output
$ZipPath = Join-Path $env:TEMP "B6.Indexer.zip"
if (Test-Path $ZipPath) { Remove-Item $ZipPath -Force }
Compress-Archive -Path (Join-Path $LocalOut '*') -DestinationPath $ZipPath -Force

# 3) Credentials + session
$Cred    = Get-Credential -Message "Enter creds for $User on $Server" -UserName $User
$Session = New-PSSession -ComputerName $Server -Credential $Cred

try {
  # 4) Prepare remote folder, stop service, preserve appsettings
  Invoke-Command -Session $Session -ArgumentList $RemoteDir,$ServiceName -ScriptBlock {
    param($RemoteDir,$ServiceName)

    if (!(Test-Path $RemoteDir)) { New-Item -ItemType Directory -Path $RemoteDir -Force | Out-Null }

    $svc = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
    if ($svc -and $svc.Status -ne 'Stopped') {
      Stop-Service $ServiceName -Force -ErrorAction SilentlyContinue
      Start-Sleep -Seconds 2
    }

    $app = Join-Path $RemoteDir 'appsettings.Production.json'
    $bak = Join-Path $RemoteDir '__appsettings.Production.json.bak'
    if (Test-Path $app) { Copy-Item $app $bak -Force }

    # Clear directory except the backup (if present)
    Get-ChildItem $RemoteDir -Force | Where-Object {
      $_.Name -notin @('appsettings.Production.json','__appsettings.Production.json.bak')
    } | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
  }

  # 5) Copy the zip (single file = no attribute headaches)
  $RemoteZip = Join-Path $RemoteDir 'package.zip'
  Copy-Item -ToSession $Session -Path $ZipPath -Destination $RemoteZip -Force

  # 6) Expand zip and restore appsettings, start service
  Invoke-Command -Session $Session -ArgumentList $RemoteDir,$ServiceName -ScriptBlock {
    param($RemoteDir,$ServiceName)
    $zip = Join-Path $RemoteDir 'package.zip'
    Expand-Archive -Path $zip -DestinationPath $RemoteDir -Force
    Remove-Item $zip -Force

    $bak = Join-Path $RemoteDir '__appsettings.Production.json.bak'
    if (Test-Path $bak) {
      Move-Item $bak (Join-Path $RemoteDir 'appsettings.Production.json') -Force
    }

    Start-Service $ServiceName
    (Get-Service $ServiceName).Status
  }

  Write-Host "Deployed and restarted $ServiceName on $Server" -ForegroundColor Green
}
finally {
  if ($Session) { Remove-PSSession $Session }
  if (Test-Path $ZipPath) { Remove-Item $ZipPath -Force }
}
