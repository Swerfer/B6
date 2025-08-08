# --- EDIT THESE ---
$Server     = '172.201.105.90'
$SiteName   = 'B6Missions.com'                  # exact IIS site name
$AppPath    = "$SiteName/api"               # iisApp path: "SiteName/virtualApp"
$User       = 'B6Missions\dennis'               # Windows user on the server
$Project    = 'C:\Users\denni\OneDrive\B6\backend\B6.Backend'
$PublishDir = 'C:\B6Missions\Backend'
$AppPool    = 'B6BackendPool'
$AllowUntrusted = $true                     # set $false if WMSvc has a trusted cert
# ------------------

$ErrorActionPreference = 'Stop'

# 1) Build & publish locally
dotnet publish $Project -c Release -o $PublishDir

# 2) Prompt for password
$Pass  = Read-Host -AsSecureString "Enter password for $User@$Server"
$BSTR  = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Pass)
$Plain = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($BSTR)

# 3) Build msdeploy args (no IIS module needed)
$msdeploy = "$env:ProgramFiles\IIS\Microsoft Web Deploy V3\msdeploy.exe"
if (-not (Test-Path $msdeploy)) {
  throw "msdeploy.exe not found at: $msdeploy. Install Web Deploy 3.6 on this machine."
}

$commonDest = "computerName=`"https://$($Server):8172/msdeploy.axd?site=$SiteName`",userName=`"$User`",password=`"$Plain`",authType=Basic"

$argsList = @(
  '-verb:sync',
  "-source:contentPath=$PublishDir",
  "-dest:contentPath=`"$AppPath`",$commonDest",
  '-enableRule:AppOffline',
  '-retryAttempts:2',
  '-usechecksum'
)
if ($AllowUntrusted) { $argsList += '-allowUntrusted' }

# 4) Push the files
& $msdeploy @argsList

Write-Host "`nDeployed. Test: https://b6missions.com/api/health" -ForegroundColor Green