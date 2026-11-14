#!/usr/bin/env bash
# Atualiza o banco com o que o time de dados publicou no data lake (Delta Lake no S3).
# Roda, nesta ordem: lugares, previsao, atividade harmonizada, grade do mapa e prata dos ultimos N dias.
# Tudo e idempotente: so entra o que mudou. Pare na primeira falha (codigo de saida != 0).
#
# Uso:  DATALAKE_BUCKET=<bucket> scripts/atualizar.sh [dias=7] [bbox-da-prata=-74,-34,-34,6|global]
# Requisitos: banco no ar (docker compose up -d), credenciais AWS locais (aws configure) e Java 21 + Maven.
# O banco vem de DB_URL / DB_USER / DB_PASSWORD (padrao: localhost:5433).
set -euo pipefail
cd "$(dirname "$0")/.."

: "${DATALAKE_BUCKET:?defina DATALAKE_BUCKET com o nome do bucket}"
DIAS="${1:-7}"
BBOX="${2:--74,-34,-34,6}"
FIM="$(date -u +%F)"
INICIO="$(date -u -d "-${DIAS} days" +%F 2>/dev/null || date -u -v-"${DIAS}"d +%F)"

rodar() { # perfil, argumentos...
  local perfil="$1"; shift
  echo ">>> $perfil $*"
  mvn -B -q spring-boot:run -Dspring-boot.run.profiles="$perfil" -Dspring-boot.run.arguments="$*"
}

echo "Janela: $INICIO a $FIM"
rodar ingest-gold "--ingest.dataset=places"
rodar ingest-gold "--ingest.dataset=forecast"
rodar ingest-gold "--ingest.dataset=harmonized --ingest.start=$INICIO --ingest.end=$FIM"
rodar ingest-gold "--ingest.dataset=map_grid --ingest.start=$INICIO --ingest.end=$FIM"
rodar ingest-silver "--ingest.start=$INICIO --ingest.end=$FIM --ingest.bbox=$BBOX"
echo "Atualizacao concluida. Conferencia: GET /api/v1/meta/sources e tabela lake_import_log."
