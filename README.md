# fire-atlas-api

API (Spring Boot) de focos de calor **MODIS e VIIRS** para o NASA Space Apps 2026
("Harmonization of MODIS and VIIRS Hot Spots"). Plataforma aberta, sem login.

```
Data lake (S3, Delta Lake do time de dados)
   prata/silver ──► importacao (profile "ingest-silver") ──► PostgreSQL + PostGIS
   gold/*       ──► Amazon Athena (catalogo Glue, consulta direta)      │  detection (particionada por ano, GiST + BRIN)
                                                                        │  sensor/satellite (catalogo estatico)
                                                                        │  lake_import_log (proveniencia da prata)
                                                                        └─ user_preference (perfil anonimo via cookie)
API REST (/api/v1 do Postgres, /api/v2 do Athena) ──► Frontend Next.js
```

A camada ouro **nao e materializada**: as rotas `/api/v2` consultam as tabelas Delta direto no S3 pelo Athena
(~12,4 GB que antes eram baixados a cada primeira carga). Motivacao, medicoes e desenho: `docs/ACESSO-DADOS-S3.md`.

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
mvn spring-boot:run             # API em http://localhost:8080 (Flyway cria o schema; a gold e sincronizada sozinha)
```

Documentacao interativa: `http://localhost:8080/swagger-ui.html` (OpenAPI em `/v3/api-docs`).

## Dados

### Camada ouro: consulta direta pelo Athena (nada a carregar)

As rotas `/api/v2` leem as tabelas Delta do catalogo Glue `fireatlas_gold` (`harmonized_cells`,
`harmonized_availability`, `map_grid`, `h3_places`, `forecast`), apontadas para `s3://<bucket>/gold/*`. Nao ha
importacao, sincronizacao nem tabela espelho no Postgres: subir a API e instantaneo e o dado novo publicado pelo time
de dados aparece na consulta seguinte.

Configuracao (todas com padrao, veja `.env.example`): `ATHENA_DATABASE` (padrao `fireatlas_gold`),
`ATHENA_WORKGROUP` (padrao `fireatlas-api`), `ATHENA_RESULT_REUSE_MINUTES` (padrao 1440) e `ATHENA_TIMEOUT_MS`.

O que isso custa e como o custo e contido:

- cada consulta nova leva ~3 s (piso do Athena) e cobra por byte escaneado (US$ 5/TB);
- **toda** consulta leva filtro de particao explicito (`resolution`, `year`/`month`): sem ele o Athena varreria a
  tabela inteira, porque os arquivos nao sao ordenados por dia;
- o SQL e canonico (celulas ordenadas, datas como literais): com `ResultReuseByAgeConfiguration` ligado, repetir a
  mesma pergunta dentro da validade responde em ~0,2 s sem escanear nada;
- o workgroup `fireatlas-api` tem `BytesScannedCutoffPerQuery` de 1 GB, que interrompe consulta fora do esperado;
- falha ou tempo esgotado no Athena vira `503` (RFC 7807), nao `500`.

Permissoes minimas do principal da API: `athena:StartQueryExecution|GetQueryExecution|GetQueryResults|StopQueryExecution|GetWorkGroup`
no workgroup, `glue:GetDatabase|GetTable|GetPartitions` no catalogo, `s3:GetObject` em `gold/*` e `s3:GetObject|PutObject`
no prefixo de resultados.

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
- `/api/v2` (gold no Athena): poda por particao (`resolution`, `year`/`month`) + reuso de resultado. Consulta nova
  ~3 s; repeticao da mesma pergunta ~0,2 s sem escanear bytes.
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
| gold | `gold/map_grid`, `gold/h3_places`, `gold/forecast`, `gold/harmonized/{cells,availability}` | Athena sobre as tabelas Delta (catalogo Glue `fireatlas_gold`), sem copia |
| prata | `prata/silver` (um foco por linha) | profile `ingest-silver` → tabela `detection` (com `record_id` e `h3_r6`) |

```bash
# prata (alimenta a harmonizacao por pareamento); padrao = Brasil, ou --ingest.bbox=minLon,minLat,maxLon,maxLat | global
mvn spring-boot:run -Dspring-boot.run.profiles=ingest-silver \
  -Dspring-boot.run.arguments="--ingest.start=2026-11-10 --ingest.end=2026-11-12"
```

- Prata: so os arquivos **ativos** do Delta entram (log + checkpoint); cada arquivo e uma transacao idempotente,
  conferida com o `numRecords` do log. A identidade do foco e o `record_id`; reimportar nao duplica. Falhas e linhas
  rejeitadas ficam em `lake_import_log`.
- Gold: a leitura e sempre da versao corrente do Delta (os arquivos removidos do log nao entram, por construcao).
- Criar o catalogo uma vez (ja aplicado nesta conta):
  `CREATE EXTERNAL TABLE fireatlas_gold.<tabela> LOCATION 's3://<bucket>/gold/<caminho>/' TBLPROPERTIES ('table_type'='DELTA')`.
  `DROP TABLE` nao funciona para Delta no Athena: use a API do Glue (`aws glue delete-table`).
- Migrations: V7–V9 (gold materializada, hoje removida), V10 (identidade da prata), V11 (estado da sincronizacao,
  hoje removido), V12 (faixas de temperatura), **V13 (derruba as tabelas da gold: a leitura passou a ser pelo Athena)**.

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
`/api/v1/predictions`: `fireatlas.prediction.source=forecast-table` (variavel `FIREATLAS_PREDICTION_SOURCE`) — nao ha
nada a importar, a leitura e direto da tabela Delta `gold/forecast`. A probabilidade vem pronta do pipeline; a resposta
informa emissao, modelo (lido do `commitInfo` do log Delta) e horizonte.

### Testes de integracao

Os testes com banco usam um PostGIS descartavel (padrao `jdbc:postgresql://localhost:55432/fireatlas_test`, `TEST_DB_URL`);
o `HarmonizationPostgisIT` roda so com `-Dtest=HarmonizationPostgisIT`.

### Atualizar quando o time de dados publicar dado novo

```bash
DATALAKE_BUCKET=<bucket> scripts/atualizar.sh            # ultimos 7 dias, prata so do Brasil
DATALAKE_BUCKET=<bucket> scripts/atualizar.sh 30 global  # ultimos 30 dias, prata do mundo
```

Importa so a prata; a gold nao precisa de atualizacao (e consultada direto no S3). Idempotente, para na primeira falha.
