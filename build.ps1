# Clean
if(Test-Path .\artifacts) { Remove-Item .\artifacts -Force -Recurse }

# Initialize
EnsurePsbuildInstalled

exec { & dotnet restore }

# Build
Invoke-MSBuild

# Test


# Package
$revision = @{ $true = $env:APPVEYOR_BUILD_NUMBER; $false = 1 }[$env:APPVEYOR_BUILD_NUMBER -ne $NULL];
$revision = "{0:D4}" -f [convert]::ToInt32($revision, 10)

exec { & dotnet pack .\src\NPoco.SqlAzure.Core -c Release -o .\artifacts --version-suffix=$revision }
