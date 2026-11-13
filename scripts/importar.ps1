# Rotina reproduzivel de importacao do data lake para o PostGIS (Windows).
# Uso: .\scripts\importar.ps1 -Start 2026-09-01 -End 2026-09-07 [-Bbox "-74,-34,-34,6"] [-Sensors "MODIS,VIIRS"]
# Requer: $env:DATALAKE_BUCKET, credenciais AWS no perfil local e o PostGIS no ar (docker compose up -d).
# Codigo de saida: 0 = importacao concluida; diferente de 0 = NAO concluida.
param(
    [Parameter(Mandatory = $true)][string]$Start,
    [Parameter(Mandatory = $true)][string]$End,
    [string]$Bbox = "-74,-34,-34,6",
    [string]$Sensors = "MODIS,VIIRS"
)
$ErrorActionPreference = "Stop"
if (-not $env:DATALAKE_BUCKET) { throw "defina `$env:DATALAKE_BUCKET (nome do bucket do data lake)" }
Set-Location (Join-Path $PSScriptRoot "..")
$jar = "target/fire-atlas-api-0.1.0.jar"
if (-not (Test-Path $jar)) { mvn -q -B -DskipTests package }
java -jar $jar --spring.profiles.active=ingest "--ingest.start=$Start" "--ingest.end=$End" "--ingest.bbox=$Bbox" "--ingest.sensors=$Sensors"
exit $LASTEXITCODE
