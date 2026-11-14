# Rotina reproduzivel de importacao da camada ouro (Delta Lake) do data lake para o PostgreSQL (Windows).
# Uso: .\scripts\importar.ps1 -Start 2026-09-01 -End 2026-09-07 [-Resolutions "0,1,2,3,4,5"]
# Requer: $env:DATALAKE_BUCKET, credenciais AWS no perfil local e o PostGIS no ar (docker compose up -d).
# Codigo de saida: 0 = importacao concluida; diferente de 0 = NAO concluida.
param(
    [Parameter(Mandatory = $true)][string]$Start,
    [Parameter(Mandatory = $true)][string]$End,
    [string]$Resolutions = ""
)
$ErrorActionPreference = "Stop"
if (-not $env:DATALAKE_BUCKET) { throw "defina `$env:DATALAKE_BUCKET (nome do bucket do data lake)" }
Set-Location (Join-Path $PSScriptRoot "..")
$jar = "target/fire-atlas-api-0.1.0.jar"
if (-not (Test-Path $jar)) { mvn -q -B -DskipTests package }
$javaArgs = @("--spring.profiles.active=ingest", "--ingest.start=$Start", "--ingest.end=$End")
if ($Resolutions) { $javaArgs += "--ingest.resolutions=$Resolutions" }
java -jar $jar @javaArgs
exit $LASTEXITCODE
