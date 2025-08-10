# --- EDIT THESE ---
$Server     = '172.201.105.90'
$SiteName   = 'B6Missions.com'             # exact IIS site name
$AppPath    = "$SiteName/api"              # iisApp path: "SiteName/virtualApp"
$User       = 'B6Missions\dennis'          # Windows user on the server
$Project    = 'C:\Users\denni\OneDrive\B6\backend\B6.Backend'
$PublishDir = 'C:\_publish\B6.Backend'     # local temp output on YOUR PC
$AllowUntrusted = $true                    # set $false if WMSvc is trusted
# ---------------

$ErrorActionPreference = 'Stop'

# 1) Build & publish locally (to a local temp folder)
dotnet publish $Project -c Release -o $PublishDir

# 2) Prompt for password
$Pass  = Read-Host -AsSecureString "Enter password for $User@$Server"
$BSTR  = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Pass)
$Plain = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($BSTR)

# 3) msdeploy path
$msdeploy = "$env:ProgramFiles\IIS\Microsoft Web Deploy V3\msdeploy.exe"
if (-not (Test-Path $msdeploy)) {
  throw "msdeploy.exe not found at: $msdeploy. Install Web Deploy 3.6."
}

$commonDest = "computerName=`"https://$($Server):8172/msdeploy.axd?site=$SiteName`",userName=`"$User`",password=`"$Plain`",authType=Basic"

# 4) Push the files
$argsList = @(
  '-verb:sync',
  "-source:contentPath=$PublishDir",
  "-dest:contentPath=`"$AppPath`",$commonDest",
  '-enableRule:AppOffline',
  '-retryAttempts:2',
  '-usechecksum'
)
if ($AllowUntrusted) { $argsList += '-allowUntrusted' }

& $msdeploy @argsList

Write-Host "`nDeployed API. Test: https://b6missions.com/api/health" -ForegroundColor Green
