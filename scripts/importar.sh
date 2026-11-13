#!/usr/bin/env bash
# Rotina reproduzivel de importacao do data lake para o PostGIS.
# Uso: scripts/importar.sh INICIO FIM [BBOX] [SENSORES]
#   scripts/importar.sh 2026-09-01 2026-09-07
#   scripts/importar.sh 2026-09-01 2026-09-07 -74,-34,-34,6 MODIS,VIIRS
# Requer: DATALAKE_BUCKET definido, credenciais AWS no perfil local e o PostGIS no ar (docker compose up -d).
# Codigo de saida: 0 = importacao concluida; diferente de 0 = NAO concluida (ver o log e GET /api/v1/meta/sources).
set -euo pipefail
cd "$(dirname "$0")/.."

START="${1:?informe a data inicial (YYYY-MM-DD)}"
END="${2:?informe a data final (YYYY-MM-DD)}"
BBOX="${3:--74,-34,-34,6}"       # Brasil por padrao
SENSORS="${4:-MODIS,VIIRS}"
: "${DATALAKE_BUCKET:?defina DATALAKE_BUCKET (nome do bucket do data lake)}"

JAR=target/fire-atlas-api-0.1.0.jar
[ -f "$JAR" ] || mvn -q -B -DskipTests package

exec java -jar "$JAR" --spring.profiles.active=ingest \
  --ingest.start="$START" --ingest.end="$END" --ingest.bbox="$BBOX" --ingest.sensors="$SENSORS"
