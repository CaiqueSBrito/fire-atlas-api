#!/usr/bin/env bash
# Atualiza o banco com a PRATA publicada pelo time de dados (Delta Lake no S3), que alimenta /api/v1.
# A camada ouro NAO e mais importada: as rotas /api/v2 consultam as tabelas Delta direto no S3 pelo Athena,
# entao nada do gold precisa ser atualizado aqui (veja docs/ACESSO-DADOS-S3.md).
# Idempotente: so entra o que mudou. Para na primeira falha (codigo de saida != 0).
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

echo "Janela: $INICIO a $FIM"
echo ">>> ingest-silver $INICIO a $FIM ($BBOX)"
mvn -B -q spring-boot:run -Dspring-boot.run.profiles=ingest-silver \
  -Dspring-boot.run.arguments="--ingest.start=$INICIO --ingest.end=$FIM --ingest.bbox=$BBOX"
echo "Atualizacao concluida. Conferencia: GET /api/v1/meta/sources e tabela lake_import_log."
