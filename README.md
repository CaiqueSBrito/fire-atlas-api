# fire-atlas-api

API (Spring Boot) de focos de calor **MODIS e VIIRS** para o NASA Space Apps 2026
("Harmonization of MODIS and VIIRS Hot Spots"). Plataforma aberta, sem login.

```
Data lake (S3, Delta Lake do time de dados)
   prata/silver ──► importacao (profile "ingest-silver") ──► PostgreSQL + PostGIS
   gold/map_grid (2026) + h3_places ──► cache mundial duravel PostgreSQL
   gold/* (demais anos/produtos)     ──► Amazon Athena (consulta direta)
   PostgreSQL: detection, sensor/satellite, lake_import_log, user_preference e cache anual
API REST (/api/v1 e /api/v2) ──► Frontend Next.js
```

A grade mundial de **2026**, sensores MODIS e VIIRS e resolucoes H3 **0–6**, e carregada uma unica vez no
Postgres compartilhado. Os demais anos/produtos continuam no Athena. O bootstrap abaixo controla a primeira
carga: nenhuma consulta de mapa, calendario de deteccoes ou estatisticas dispara um download anual por visitante.

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
mvn spring-boot:run             # API em http://localhost:8080; Flyway cria o schema, POST inicia o cache anual
```

Documentacao interativa: `http://localhost:8080/swagger-ui.html` (OpenAPI em `/v3/api-docs`).

## Dados

### Cache mundial compartilhado de 2026

`POST /api/v2/bootstrap/2026` inicia ou acompanha a carga em background (202 enquanto carregando, 200 quando
pronto); `GET /api/v2/bootstrap/2026` so le o estado. Ambos retornam `Cache-Control: no-store` e:

```json
{"state":"loading","completedUnits":3,"totalUnits":15,"rowsLoaded":32000,"message":"...","error":null}
```

`state` e `idle`, `loading`, `ready` ou `failed`. `completedUnits` conta **arquivos/particoes integralmente
confirmados**, nunca requisicoes HTTP, dias presumidos nem porcentagem simulada. `totalUnits` e o plano exato
dos objetos Parquet **projetados pelo Athena**, para `map_grid` de 2026 e `h3_places`, em H3 0–6.
Durante a exportacao, `message` informa particoes Athena verificadas; isso nao e publicacao no PostGIS.
Uma resolucao vazia exige confirmacao pelo snapshot/manifestos. `rowsLoaded` soma linhas de grade publicadas
(nao linhas de lugares nem lotes temporarios). Durante arquivos grandes, `message` informa as linhas persistidas
em staging. `ready` exige todas as unidades; lacunas de datas e dias futuros nao recebem linhas artificiais.

A carga primeiro executa `UNLOAD (SELECT colunas_explicitas ... WHERE resolution=... AND year=2026)` no Athena,
com saida **Parquet ZSTD**. A grade exporta somente dez colunas consumidas: dia, sensor, celula, contagem, FRP
total/maximo e classes/medias de confianca. Lugares preservam doze colunas, incluindo latitude/longitude da gold.
`platforms`, `frp_mean_mw` e `brightness_max_k` nao sao transferidas nem persistidas no cache.
Nao ha resultado anual CSV nem fallback para baixar o Parquet original. O pequeno CSV de manifesto contem
apenas os caminhos dos arquivos exportados, nao os dados do ano.

`BOOTSTRAP_EXPORT_CONCURRENCY=2` limita UNLOADs simultaneos (1–4); `BOOTSTRAP_EXPORT_TIMEOUT_MS=300000`
limita a espera por consulta. `BOOTSTRAP_EXPORT_PREFIX` vazio deriva do OutputLocation do workgroup,
acrescentando `bootstrap/2026/`. O prefixo deve ser exclusivo das exportacoes. Tokens, IDs e manifestos
sao persistidos em `annual_athena_export`, permitindo retomar a mesma consulta sem exportar novamente.
Tentativas terminadas com falha usam destinos novos e vazios. Versoes Delta sao verificadas antes/depois:
mudanca durante o UNLOAD impede publicar um conjunto inconsistente.

Depois, `BOOTSTRAP_IMPORT_WORKERS=2` limita downloads/importadores simultaneos (1–4). Cada trabalhador tem
seu temporario, conexao PostgreSQL, DuckDB (128 MB/uma thread) e staging por arquivo; grava lotes de 1.000.
O pool Hikari exige pelo menos trabalhadores+3 conexoes. A falha cancela tarefas e aguarda todos os escritores
terminarem antes de liberar o advisory lock. `ready` so aparece apos todas as publicacoes confirmadas.
O teto de bytes do workgroup continua valendo: dividir consultas por mes nao garante reduzir a leitura
de uma tabela particionada por resolucao/ano.

Permissoes adicionais: `athena:GetWorkGroup`, operacoes de execucao/leitura/cancelamento Athena, leitura Glue/gold,
`s3:ListBucket` e `s3:GetObject|PutObject` nos destinos/resultados, e KMS quando aplicavel. Nao e necessario
`s3:DeleteObject` para carregar; exportacoes obsoletas/orfas precisam de limpeza administrativa ou lifecycle.
Nao expire objetos ainda referenciados por cargas retomaveis. A credencial local permitiu UNLOAD, mas negou
DeleteObject dos probes; esses objetos pequenos permanecem no prefixo de teste.
UNLOAD acrescenta custo de consulta Athena, requisicoes e armazenamento S3; nao ha ganho de tempo garantido.
Referencia: https://docs.aws.amazon.com/athena/latest/ug/unload.html

O Postgres conserva plano, dados e marcadores. Um advisory lock de sessao garante single-flight inclusive entre
instancias da API. Cada arquivo so aparece na grade junto com seu marcador em uma transacao; lotes parciais
ficam isolados no staging. Depois de falha/reinicio, outro POST revalida o manifesto, reaproveita arquivos
confirmados e descarta staging incompleto, sem duplicar contagens. GET identifica uma carga interrompida sem
iniciar trabalho. A API publica uma mensagem segura de erro; detalhes completos ficam no log.

O cache usa **PostgreSQL com PostGIS**. A migracao V15 preserva `latitude`/`longitude` publicadas em `h3_places`,
gera `geom geometry(Point, 4326)` e cria `annual_h3_places_geom_gist` para consultas espaciais. As coordenadas
representam o centro da celula H3, nao a posicao de cada incendio. Nao ha conversao de H3 em coordenadas na API:
o banco construi os pontos a partir das coordenadas da gold. A grade diaria referencia esse catalogo por resolucao
e celula, sem repetir geometria para cada dia/sensor. A publicacao usa colunas explicitas para que somente o
PostGIS calcule a coluna gerada; staging incompleto nao e consultado. Um cache antigo sem coordenadas republica
apenas os arquivos de lugares, preservando a grade anual ja confirmada.

`BOOTSTRAP_REFRESH_HOURS` (padrao **6**, minimo 1) limita a revalidacao via POST. Cache pronto recente nao usa
AWS. Quando vence, o POST verifica snapshots; exportacoes completas do mesmo snapshot/projecao e arquivos
ja publicados sao reutilizados. Um snapshot novo gera outra exportacao projetada e substitui o plano anterior.
A migracao V16 invalida o cache bruto antigo na troca de formato. GET nao inicia nem atualiza cargas.

Mapas, totais diarios/por periodo, ranking, distribuicao/percentis, comparacao/confianca e catalogo de lugares
usam o cache pronto. Consultas de 2026 antes de `ready` retornam 503, sem tentar Athena nem iniciar carga.
Filtros por celulas, pais (ISO a2/a3), continente e cidade/raio continuam na mesma resolucao. Periodos que cruzam
2026 leem apenas os outros anos no Athena em paginas, unem em tabela temporaria e agregam o conjunto inteiro
no Postgres **antes** de limite/ranking/percentil; nao ha soma de top-Ns nem percentis parciais. Consultas
inteiramente fora de 2026 continuam no Athena. Leituras do cache usam snapshot transacional consistente.

### Demais dados gold: consulta direta pelo Athena

Os demais anos de `map_grid`, `harmonized_cells`, `harmonized_availability` e `forecast` continuam nas tabelas
Delta do catalogo Glue `fireatlas_gold`, apontadas para `s3://<bucket>/gold/*`.

Configuracao: `ATHENA_DATABASE` (padrao `fireatlas_gold`), `ATHENA_WORKGROUP` (padrao `fireatlas-api`),
`ATHENA_RESULT_REUSE_MINUTES` (padrao 1440) e `ATHENA_TIMEOUT_MS`; veja `.env.example`.

O que isso custa e como o custo e contido:

- cada consulta nova leva ~3 s (piso do Athena) e cobra por byte escaneado (US$ 5/TB);
- **toda** consulta leva filtro de particao explicito (`resolution`, `year`/`month`): sem ele o Athena varreria a
  tabela inteira, porque os arquivos nao sao ordenados por dia;
- o SQL e canonico (celulas ordenadas, datas como literais): com `ResultReuseByAgeConfiguration` ligado, repetir a
  mesma pergunta dentro da validade responde em ~0,2 s sem escanear nada;
- a expansao de uma regiao fica limitada a 12.000 celulas no filtro SQL, deixando margem sob o teto de
  262.144 caracteres do Athena. Se necessario, a cobertura usa uma resolucao H3 mais grossa, sem truncar a
  regiao; os limites de resposta (20.000 celulas no mapa) e de agregacao (5.000) continuam independentes.
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
- `/api/v2` (grade 2026): cache PostgreSQL com indices por resolucao/data/celula e lugares; nenhuma consulta
  anual Athena depois do bootstrap. Demais anos/produtos: poda por particao + reuso de resultado Athena.
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
| gold | `gold/map_grid`, `gold/h3_places`, `gold/forecast`, `gold/harmonized/{cells,availability}` | Cache PostgreSQL para grade 2026/lugares; demais anos/produtos pelo Athena |
| prata | `prata/silver` (um foco por linha) | profile `ingest-silver` → tabela `detection` (com `record_id` e `h3_r6`) |

```bash
# prata (alimenta a harmonizacao por pareamento); padrao = Brasil, ou --ingest.bbox=minLon,minLat,maxLon,maxLat | global
mvn spring-boot:run -Dspring-boot.run.profiles=ingest-silver \
  -Dspring-boot.run.arguments="--ingest.start=2026-11-10 --ingest.end=2026-11-12"
```

- Prata: so os arquivos **ativos** do Delta entram (log + checkpoint); cada arquivo e uma transacao idempotente,
  conferida com o `numRecords` do log. A identidade do foco e o `record_id`; reimportar nao duplica. Falhas e linhas
  rejeitadas ficam em `lake_import_log`.
- Gold: o cache fixa snapshots Delta completos e revalida por POST; consultas diretas usam a versao corrente do Athena.
- Criar o catalogo uma vez (ja aplicado nesta conta):
  `CREATE EXTERNAL TABLE fireatlas_gold.<tabela> LOCATION 's3://<bucket>/gold/<caminho>/' TBLPROPERTIES ('table_type'='DELTA')`.
  `DROP TABLE` nao funciona para Delta no Athena: use a API do Glue (`aws glue delete-table`).
- Migrations: V7–V9 (antiga materializacao), V10 (identidade da prata), V11 (antigo estado de sincronizacao),
  V12 (temperatura), V13 (remove materializacao antiga), **V14 (cache anual compartilhado, staging e progresso)**.

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

O script importa so a prata, idempotente, parando na primeira falha. Para atualizar a grade 2026, use
`POST /api/v2/bootstrap/2026` apos o intervalo de revalidacao; os demais produtos gold seguem consultados no S3.
