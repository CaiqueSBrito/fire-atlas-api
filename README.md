# fire-atlas-api

API (Spring Boot) de focos de calor **MODIS e VIIRS** para o NASA Space Apps 2026
("Harmonization of MODIS and VIIRS Hot Spots"). Plataforma aberta, sem login.

```
Data lake (S3, Delta Lake do time de dados)
   gold/*       ──► scripts/atualizar.sh (profile "ingest-gold") ──► PostgreSQL + PostGIS
   prata/silver ──► importacao (profile "ingest-silver")         ──►   │  daily_h3, h3_place, forecast, harmonized_* (gold)
                                                                       │  detection (prata, particionada por ano, GiST + BRIN)
                                                                       │  sensor/satellite (catalogo estatico)
                                                                       │  lake_import_log (proveniencia das importacoes)
                                                                       └─ user_preference (perfil anonimo via cookie)
API REST (/api/v1 e /api/v2, so PostgreSQL) ──► Frontend Next.js
```

A camada ouro e **baixada pelo `scripts/atualizar.sh`** para o PostgreSQL; as rotas `/api/v2` e o front leem somente o
que o script carregou. Nenhuma requisicao consulta o data lake (nem o Athena) em tempo de resposta.

## Rodar localmente

Tudo no Docker (so precisa de Docker; o build compila com Maven dentro da imagem):

```bash
cp .env.example .env           # preencha DATALAKE_BUCKET e, se usar, AWS_* (o compose repassa o .env para a API)
docker compose up -d --build   # PostGIS em localhost:5433 + API em http://localhost:8080
```

Dentro do compose a API usa `db:5432`; o `DB_URL` do `.env` so vale para rodar fora do Docker.

Ou a API pelo Maven (requisitos: Java 21+, Maven, Docker):

```bash
cp .env.example .env           # preencha DATALAKE_BUCKET (o .env e lido pela API, nao precisa exportar)
docker compose up -d db         # so o PostGIS em localhost:5433 (evita conflito com outro Postgres na 5432)
mvn spring-boot:run             # API em http://localhost:8080 (Flyway cria o schema; dados entram pelo atualizar.sh)
```

Documentacao interativa: `http://localhost:8080/swagger-ui.html` (OpenAPI em `/v3/api-docs`).

## Dados

### Camada ouro: `scripts/atualizar.sh` (alimenta a `/api/v2`)

```bash
scripts/atualizar.sh                 # gold dos ultimos 7 dias, resolucoes H3 0 a 6
scripts/atualizar.sh 30              # ultimos 30 dias
scripts/atualizar.sh 30 0,1,2,3,4    # so algumas resolucoes (carga menor)
```

O script le `DATALAKE_BUCKET` do ambiente ou do `.env` e roda o profile `ingest-gold` uma vez, com
`--ingest.dataset=all`: grade do mapa (`gold/map_grid` → `daily_h3`), lugares (`gold/h3_places` → `h3_place`),
previsao (`gold/forecast` → `forecast`/`forecast_run`) e atividade harmonizada
(`gold/harmonized/{cells,availability}` → `harmonized_cell`/`harmonized_availability`). Grade e atividade entram so na
janela pedida; lugares e previsao entram inteiros.

- So os arquivos **ativos** do Delta entram (log + checkpoint); cada particao e uma transacao conferida com o
  `numRecords` do log. Reimportar a mesma janela substitui as linhas, sem duplicar.
- Cada importacao (concluida ou falha) fica em `lake_import_log`: `GET /api/v2/meta/sources` lista e o front deduz dai
  as resolucoes e o periodo disponiveis. `GET /api/v2/meta/coverage` traz o periodo por sensor.
- Para na primeira falha (codigo de saida != 0). Sem dado importado, as rotas respondem vazio com `message`.

Permissoes minimas do principal que roda o script: `s3:ListBucket` no bucket e `s3:GetObject` em `gold/*`.

### Camada prata: importacao para o Postgres (alimenta a `/api/v1`)

A prata (`prata/silver`, um foco por linha) continua sendo importada para `detection`:

```bash
export DATALAKE_BUCKET=<bucket>   # PowerShell: $env:DATALAKE_BUCKET="<bucket>"; ou no .env
mvn spring-boot:run -Dspring-boot.run.profiles=ingest-silver   -Dspring-boot.run.arguments="--ingest.start=2026-11-10 --ingest.end=2026-11-12"
```

- So entram as linhas dentro do periodo (e da regiao, se informada); cada arquivo do Delta e uma transacao idempotente.
- Cada importacao registra arquivo de origem, regiao, periodo, contagens e linhas rejeitadas em `lake_import_log`
  (`GET /api/v1/meta/sources` lista as da prata).

Credenciais AWS: prefira o perfil local (`aws configure`/`aws login`) ou IAM Role. `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`
(e `AWS_SESSION_TOKEN`) tambem sao aceitas pelo ambiente ou pelo `.env` (fora do git; use um usuario so leitura);
**nunca** no codigo. `DATALAKE_BUCKET` aceita o nome do bucket ou `s3://nome`.

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

- `/api/v1` (prata no Postgres): tabela particionada por ano + indice GiST (geometria) + BRIN (data); toda consulta usa
  bbox + datas.
- `/api/v2` (gold no Postgres): chave primaria por `(res, cell, ...)` e indice por `(res, data)`; toda consulta usa uma
  resolucao e o periodo.
- Payload limitado (5000 pontos / 20000 celulas, com flag `truncated`), gzip e `Cache-Control`.

## Cookies / perfil anonimo

O front deve chamar a API com `credentials: "include"`. Se front e API estiverem em dominios diferentes (HTTPS),
use `COOKIE_SAME_SITE=None` e `COOKIE_SECURE=true`, e inclua o dominio do front em `CORS_ORIGINS`.

## Testes

```bash
mvn test
```

## Integracao com o data lake do time de dados (Delta Lake)

O backend so **le** e **consulta**; nao converte coordenadas em H3 (o indice H3 vem pronto do pipeline) e nao
recalcula previsao nem harmonizacao.

| Camada | Fonte | Como e servida |
|---|---|---|
| gold | `gold/map_grid`, `gold/h3_places`, `gold/forecast`, `gold/harmonized/{cells,availability}` | `scripts/atualizar.sh` (profile `ingest-gold`) → PostgreSQL |
| prata | `prata/silver` (um foco por linha) | profile `ingest-silver` → tabela `detection` (com `record_id` e `h3_r6`) |

```bash
# prata (alimenta a harmonizacao por pareamento); padrao = Brasil, ou --ingest.bbox=minLon,minLat,maxLon,maxLat | global
mvn spring-boot:run -Dspring-boot.run.profiles=ingest-silver \
  -Dspring-boot.run.arguments="--ingest.start=2026-11-10 --ingest.end=2026-11-12"
```

- Prata: so os arquivos **ativos** do Delta entram (log + checkpoint); cada arquivo e uma transacao idempotente,
  conferida com o `numRecords` do log. A identidade do foco e o `record_id`; reimportar nao duplica. Falhas e linhas
  rejeitadas ficam em `lake_import_log`.
- Gold: so os arquivos ativos da versao corrente do Delta entram; a API serve o que o ultimo `atualizar.sh` carregou.
- Migrations: V7–V9 (gold materializada), V10 (identidade da prata), V11 (estado da sincronizacao, removido), V12
  (faixas de temperatura), V13 (removia a gold do banco na fase Athena), **V14 (recria as tabelas da gold carregadas
  pelo `atualizar.sh`)**.

### Rotas gold (`/api/v2`)

Mapa (`/detections`, `/coverage`), calendario, ranking, analise, `/forecast`, lugares (`country`, `continent`, `city` + `cityRadiusKm`),
sensores e `/activity/{calendar,monthly,anomaly,availability}` (atividade harmonizada: **celulas ativas equivalentes ao MODIS Aqua**,
nao e contagem de focos; `contributing = 0` significa sem dado). Documentacao completa no Swagger.

Cada dia de `/activity/calendar` traz `basis` (`viirs`, `aqua` ou `terra`: sensor que mediu o brilho) e `temperature`: 7
faixas de 10 K da temperatura de brilho (`lt320`, `320`…`350`, `ge360`, `none`), cada uma com `fromK`, `toK` e
`harmonized`; somam o `harmonized` do dia. Vem `null` em dia sem dado ou quando os arquivos da gold nao tem as colunas
`harmonized_t_*` (arquivos antigos continuam validos).

`/forecast` aceita `cells` de qualquer resolucao (a celula de resolucao 6 entra pelo ancestral, sem expandir filhas) ou um
lugar, e devolve `summary` (celulas, ativas, maior e media da probabilidade na regiao inteira, sem os filtros da lista) e
as celulas da maior para a menor probabilidade com `countryName`, `cityName` e `cityDistanceKm` de `h3_places`.

### Previsao a partir da gold/forecast

Por padrao o provedor preditivo continua desligado (`MODEL_NOT_VALIDATED`). Para servir a previsao do time de dados em
`/api/v1/predictions`: `fireatlas.prediction.source=forecast-table` (variavel `FIREATLAS_PREDICTION_SOURCE`). A leitura e da
tabela `forecast` carregada pelo `scripts/atualizar.sh`. A probabilidade vem pronta do pipeline; a resposta informa
emissao, modelo (lido do `commitInfo` do log Delta na importacao) e horizonte.

### Testes de integracao

Os testes com banco usam um PostGIS descartavel (padrao `jdbc:postgresql://localhost:55432/fireatlas_test`, `TEST_DB_URL`);
o `HarmonizationPostgisIT` roda so com `-Dtest=HarmonizationPostgisIT`.

### Atualizar quando o time de dados publicar dado novo

```bash
scripts/atualizar.sh        # gold dos ultimos 7 dias
scripts/atualizar.sh 30     # gold dos ultimos 30 dias
```

Baixa so a gold. A prata (`/api/v1`) continua no profile `ingest-silver` (secao acima). Idempotente, para na primeira falha.
