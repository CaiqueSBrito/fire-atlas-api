# fire-atlas-api

API (Spring Boot) de focos de calor **MODIS e VIIRS** para o NASA Space Apps 2026
("Harmonization of MODIS and VIIRS Hot Spots"). Plataforma aberta, sem login.

```
Data lake (S3, Delta Lake do time de dados: prata/silver e gold/*)
        │  importacao por periodo (profiles "ingest-silver" e "ingest-gold")
        ▼
PostgreSQL + PostGIS  ── detection (particionada por ano, GiST + BRIN)
        │               ├─ daily_grid (grade 0,1°/dia pre-agregada)
        │               ├─ lake_import_log (proveniencia das importacoes)
        ▼               └─ user_preference (perfil anonimo via cookie)
API REST (/api/v1) ──► Frontend Next.js
```

## Rodar localmente

Requisitos: Java 21+, Maven, Docker.

```bash
docker compose up -d            # PostGIS em localhost:5433 (evita conflito com outro Postgres na 5432)
mvn spring-boot:run             # API em http://localhost:8080 (Flyway cria o schema)
```

Documentacao interativa: `http://localhost:8080/swagger-ui.html` (OpenAPI em `/v3/api-docs`).

## Carregar dados do data lake

As credenciais AWS vem do perfil local (`aws configure`) ou de IAM Role — **nunca** coloque chaves no codigo ou no `.env`.
Os dados vem das tabelas Delta publicadas pelo time de dados no S3 (veja a secao "Integracao com o data lake" abaixo):
a **prata** (`prata/silver`, um foco por linha) alimenta `detection`, e o **gold** alimenta o mapa agregado, os lugares, a
previsao e a atividade harmonizada.

```bash
export DATALAKE_BUCKET=<bucket>   # PowerShell: $env:DATALAKE_BUCKET="<bucket>"
mvn spring-boot:run -Dspring-boot.run.profiles=ingest-silver   -Dspring-boot.run.arguments="--ingest.start=2026-11-10 --ingest.end=2026-11-12"
```

- So entram as linhas dentro do periodo (e da regiao, se informada); cada arquivo do Delta e uma transacao idempotente.
- Cada importacao registra arquivo de origem, regiao, periodo, contagens e linhas rejeitadas em `lake_import_log`
  (`GET /api/v1/meta/sources` lista as da prata).

## Endpoints (`/api/v1`)

| Metodo | Rota | O que faz |
|---|---|---|
| GET | `/detections?bbox&start&end&sensor&zoom` | Pontos (zoom ≥ 9) ou celulas agregadas (zoom < 9) |
| GET | `/calendar?bbox&month=YYYY-MM&sensor` | Total por dia/sensor (alimenta as "cartas") |
| GET | `/compare?bbox&start&end&sensor` | Estatisticas por sensor + aviso de nao comparabilidade |
| GET | `/timeseries?bbox&start&end&sensor` | Serie diaria e dias de atividade incomum |
| GET | `/harmonized?bbox&start&end` | Harmonizacao MODIS×VIIRS (ver `docs/HARMONIZACAO.md`) |
| GET | `/predictions?bbox&start&end` | Previsoes de focos (`MODEL_NOT_VALIDATED`; ver `docs/PREDICOES.md`) |
| GET/PUT | `/preferences` | Regiao favorita do usuario anonimo (cookie `fa_uid`) |
| GET | `/meta/sources`, `/meta/coverage` | Rastreabilidade e cobertura |

`bbox` = `minLon,minLat,maxLon,maxLat`. `sensor` = `MODIS | VIIRS | BOTH`. Sem resultados, a resposta traz `message`.
Erros de validacao retornam `400` (RFC 7807).

## Performance

- Tabela particionada por ano + indice GiST (geometria) + BRIN (data): toda consulta usa bbox + datas.
- Zoom afastado e calendario leem `daily_grid` (pre-agregada), nunca as deteccoes brutas.
- Payload limitado (5000 pontos / 20000 celulas, com flag `truncated`), gzip e `Cache-Control`.

## Cookies / perfil anonimo

O front deve chamar a API com `credentials: "include"`. Se front e API estiverem em dominios diferentes (HTTPS),
use `COOKIE_SAME_SITE=None` e `COOKIE_SECURE=true`, e inclua o dominio do front em `CORS_ORIGINS`.

## Testes

```bash
mvn test
```

## Integracao com o data lake do time de dados (Delta Lake)

A API importa as tabelas Delta publicadas pelo time de dados. O backend so **le** e **consulta**;
nao converte coordenadas em H3 (o indice H3 vem pronto do pipeline) e nao recalcula previsao nem harmonizacao.

| Profile | Fonte | Destino | Comando |
|---|---|---|---|
| `ingest-gold` | `gold/map_grid`, `gold/h3_places`, `gold/forecast`, `gold/harmonized` | `daily_h3`, `h3_place`, `forecast`, `harmonized_cell`, `harmonized_availability` | abaixo |
| `ingest-silver` | `prata/silver` (um foco por linha) | `detection` (com `record_id` e `h3_r6`) | abaixo |

```bash
# gold: --ingest.dataset = map_grid | places | forecast | harmonized | all
mvn spring-boot:run -Dspring-boot.run.profiles=ingest-gold \
  -Dspring-boot.run.arguments="--ingest.dataset=map_grid --ingest.start=2024-08-01 --ingest.end=2024-09-30 --ingest.resolutions=0,1,2,3,4,5,6"

# prata (alimenta a harmonizacao por pareamento); padrao = Brasil, ou --ingest.bbox=minLon,minLat,maxLon,maxLat | global
mvn spring-boot:run -Dspring-boot.run.profiles=ingest-silver \
  -Dspring-boot.run.arguments="--ingest.start=2026-11-10 --ingest.end=2026-11-12"
```

- So os arquivos **ativos** do Delta entram (log + checkpoint). Cada arquivo e uma transacao, idempotente, conferida com o
  `numRecords` do log; falhas e linhas rejeitadas (ex.: FRP negativo) ficam em `lake_import_log`.
- Prata: a identidade do foco e o `record_id`; reimportar nao duplica.
- Migrations: V7–V9 (gold), V10 (identidade da prata e `forecast_run`).

### Rotas gold (`/api/v2`)

Mapa (`/detections`, `/coverage`), calendario, ranking, analise, `/forecast`, lugares (`country`, `continent`, `city` + `cityRadiusKm`),
sensores e `/activity/{calendar,monthly,anomaly,availability}` (atividade harmonizada: **celulas ativas equivalentes ao MODIS Aqua**,
nao e contagem de focos; `contributing = 0` significa sem dado). Documentacao completa no Swagger.

### Previsao a partir da gold/forecast

Por padrao o provedor preditivo continua desligado (`MODEL_NOT_VALIDATED`). Para servir a previsao do time de dados em
`/api/v1/predictions`: `fireatlas.prediction.source=forecast-table` (variavel `FIREATLAS_PREDICTION_SOURCE`) depois de importar
`--ingest.dataset=forecast`. A probabilidade vem pronta do pipeline; a resposta informa emissao, modelo (de `forecast_run`) e horizonte.

### Testes de integracao

Os testes com banco usam um PostGIS descartavel (padrao `jdbc:postgresql://localhost:55432/fireatlas_test`, `TEST_DB_URL`);
o `HarmonizationPostgisIT` roda so com `-Dtest=HarmonizationPostgisIT`.

### Atualizar quando o time de dados publicar dado novo

```bash
DATALAKE_BUCKET=<bucket> scripts/atualizar.sh            # ultimos 7 dias, prata so do Brasil
DATALAKE_BUCKET=<bucket> scripts/atualizar.sh 30 global  # ultimos 30 dias, prata do mundo
```

Roda lugares, previsao, atividade harmonizada, grade do mapa e prata; e idempotente (so entra o que mudou) e para na primeira falha.
