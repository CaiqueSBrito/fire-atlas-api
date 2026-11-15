#!/usr/bin/env bash
# Baixa a camada ouro publicada pelo time de dados (Delta Lake no S3) para o PostgreSQL, que serve /api/v2:
# lugares (gold/h3_places), previsao (gold/forecast), atividade harmonizada (gold/harmonized/{cells,availability})
# e grade do mapa (gold/map_grid) dos ultimos N dias. A API e o front leem somente o que este script carregou.
# Idempotente: cada arquivo do Delta e uma transacao conferida com o numRecords do log; reimportar substitui.
# Para na primeira falha (codigo de saida != 0).
#
# Uso:  DATALAKE_BUCKET=<bucket> scripts/atualizar.sh [dias=7] [resolucoes=0,1,2,3,4,5,6]
# Requisitos: banco no ar (docker compose up -d db), credenciais AWS locais (aws configure) e Java 21 + Maven.
# O banco vem de DB_URL / DB_USER / DB_PASSWORD (padrao: localhost:5433). O .env da raiz tambem e lido.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ -z "${DATALAKE_BUCKET:-}" ] && [ -f .env ]; then
  DATALAKE_BUCKET="$(sed -n 's/^DATALAKE_BUCKET=//p' .env | tail -n 1)"
fi
: "${DATALAKE_BUCKET:?defina DATALAKE_BUCKET com o nome do bucket}"
export DATALAKE_BUCKET
DIAS="${1:-7}"
RESOLUCOES="${2:-}"
FIM="$(date -u +%F)"
INICIO="$(date -u -d "-${DIAS} days" +%F 2>/dev/null || date -u -v-"${DIAS}"d +%F)"

ARGS="--ingest.dataset=all --ingest.start=$INICIO --ingest.end=$FIM"
[ -n "$RESOLUCOES" ] && ARGS="$ARGS --ingest.resolutions=$RESOLUCOES"

echo "Janela da gold: $INICIO a $FIM"
mvn -B -q spring-boot:run -Dspring-boot.run.profiles=ingest-gold -Dspring-boot.run.arguments="$ARGS"
echo "Atualizacao concluida. Conferencia: GET /api/v2/meta/sources e GET /api/v2/meta/coverage."
