#!/usr/bin/env bash
# Rotina reproduzivel de importacao da camada ouro (Delta Lake) do data lake para o PostgreSQL.
# Uso: scripts/importar.sh INICIO FIM [RESOLUCOES]
#   scripts/importar.sh 2026-09-01 2026-09-07
#   scripts/importar.sh 2023-01-01 2026-12-31 0,1,2,3,4,5
# Requer: DATALAKE_BUCKET definido, credenciais AWS no perfil local e o PostGIS no ar (docker compose up -d).
# Codigo de saida: 0 = importacao concluida; diferente de 0 = NAO concluida (ver o log e GET /api/v1/meta/sources).
set -euo pipefail
cd "$(dirname "$0")/.."

START="${1:?informe a data inicial (YYYY-MM-DD)}"
END="${2:?informe a data final (YYYY-MM-DD)}"
RESOLUTIONS="${3:-}"             # vazio = 0 a 6
: "${DATALAKE_BUCKET:?defina DATALAKE_BUCKET (nome do bucket do data lake)}"

JAR=target/fire-atlas-gold-api-0.1.0.jar
[ -f "$JAR" ] || mvn -q -B -DskipTests package

ARGS=(--spring.profiles.active=ingest --ingest.start="$START" --ingest.end="$END")
[ -n "$RESOLUTIONS" ] && ARGS+=(--ingest.resolutions="$RESOLUTIONS")
exec java -jar "$JAR" "${ARGS[@]}"
