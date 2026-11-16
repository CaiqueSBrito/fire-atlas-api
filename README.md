# fire-atlas-api

API (Spring Boot) de focos de calor **MODIS e VIIRS** para o NASA Space Apps 2026
("Harmonization of MODIS and VIIRS Hot Spots"). Plataforma aberta, sem login.

```
Data lake (S3, Delta Lake do time de dados)
   prata/silver ──► importacao (profile "ingest-silver") ──► PostgreSQL + PostGIS
   gold/map_grid (2026, depois 2025 → anos anteriores) + h3_places ──► cache PostgreSQL
   gold/forecast + lugares r6 ──► UNLOAD mensal ──► cache nativo de previsao no PostgreSQL/PostGIS
   gold/* (outros produtos/anos ainda nao publicados no cache) ──► Amazon Athena
   PostgreSQL: detection, sensor/satellite, lake_import_log, user_preference e cache anual
API REST (/api/v1 e /api/v2) ──► Frontend Next.js
```

A grade mundial de **2026**, sensores MODIS e VIIRS e resolucoes H3 **0–5**, libera a interface primeiro.
Depois, o mesmo pipeline carrega **2025, 2024 e os demais anos existentes no manifesto Delta**, em ordem
decrescente, um ano por vez, no Postgres compartilhado. Nenhuma consulta de exploracao inicia download anual
por visitante; a fila pertence ao servidor e continua depois de fechar a aba.

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

### Carga anual compartilhada: 2026 primeiro, historico em segundo plano

`POST /api/v2/bootstrap/2026` inicia ou acompanha a carga em background (202 enquanto carregando, 200 quando
pronto); `GET /api/v2/bootstrap/2026` so le o estado. O `ready` de 2026 libera o frontend e nao aguarda o historico.
O mesmo POST inicia ou retoma a fila historica; visitas concorrentes nao duplicam a carga. Anos ja prontos sao
pulados. `GET /api/v2/bootstrap/{ano}` consulta passivamente o progresso de qualquer ano anterior a 2026:
sem trabalho iniciado, retorna `idle` sem criar plano nem acessar AWS. Todos retornam `Cache-Control: no-store` e:

```json
{"state":"loading","completedUnits":3,"totalUnits":15,"rowsLoaded":32000,"message":"...","error":null}
```

`state` e `idle`, `loading`, `ready` ou `failed`. `completedUnits` conta **arquivos/particoes integralmente
confirmados**, nunca requisicoes HTTP, dias presumidos nem porcentagem simulada. `totalUnits` e o plano exato
dos objetos Parquet **projetados pelo Athena** para o ano em execucao, em H3 0–5. O catalogo `h3_places`
e compartilhado e carregado junto de 2026, nao baixado de novo para cada ano historico.
Durante a exportacao, `message` informa particoes Athena verificadas; isso nao e publicacao no PostGIS.
Uma resolucao vazia exige confirmacao pelo snapshot/manifestos. `rowsLoaded` soma linhas de grade publicadas
(nao linhas de lugares nem de arquivos ainda abertos). Durante arquivos grandes, `message` informa as linhas ja
enviadas ao COPY, visiveis so no commit do arquivo; o coordenador publica essa mensagem no maximo 1x/s.
`ready` exige todas as unidades; lacunas de datas e dias futuros nao recebem linhas artificiais.

A carga primeiro executa no Athena **um UNLOAD por mes de cada resolucao** da grade
(`UNLOAD (SELECT colunas_explicitas ... WHERE resolution=R AND year=ANO AND day >= DATE 'inicio_do_mes'
AND day < DATE 'inicio_do_mes_seguinte')`, 12 por resolucao, 72 unidades de grade por ano)
e, na carga de 2026, exporta lugares sem periodo (`h3_places` nao tem ano/mes). Acima de 300 mil linhas por resolucao,
lugares sao divididos em `ceil(linhas/300000)` partes, no maximo 8, por hash de `h3_cell` modulo o numero
de partes. Cada celula pertence a exatamente uma parte; a quantidade e uma meta, nao um limite por arquivo.
A medicao anterior com H3 0–6 tinha 99 unidades planejadas e 92 UNLOADs efetivos; esses numeros preservam
o escopo historico do benchmark, nao o plano atual. Saida **Parquet ZSTD**. A grade exporta somente dez colunas: dia, sensor, celula, contagem, FRP
total/maximo e classes/medias de confianca. Lugares preservam doze colunas, incluindo latitude/longitude da gold.
`platforms`, `frp_mean_mw` e `brightness_max_k` nao sao transferidas nem persistidas no cache.
Nao ha resultado anual CSV nem fallback para baixar o Parquet original. O pequeno CSV de manifesto contem
apenas os caminhos dos arquivos exportados, nao os dados do ano.

`BOOTSTRAP_EXPORT_CONCURRENCY=12` limita UNLOADs simultaneos (1–12): por padrao os 12 meses de uma resolucao
rodam em paralelo, cobrindo o ano inteiro de uma vez. `BOOTSTRAP_EXPORT_TIMEOUT_MS=300000`
limita a espera por consulta (cada mes e uma consulta); mes sem linhas no snapshot Delta nao gera consulta.
`BOOTSTRAP_EXPORT_PREFIX` vazio deriva do OutputLocation do workgroup,
acrescentando `bootstrap/ANO/`; um prefixo configurado recebe `ANO/`. Tokens, IDs e manifestos
sao persistidos em `annual_athena_export` por (snapshot, tipo, resolucao, mes, parte), permitindo retomar cada
mes/parte sem exportar novamente os concluidos. O fingerprint inclui **o ano** e a regra de divisao para nunca
reutilizar uma exportacao de outro ano ou corte. A migracao V19 acrescenta a chave da parte.
Tentativas terminadas com falha usam destinos novos e vazios. Versoes Delta sao verificadas antes/depois:
mudanca durante o UNLOAD impede publicar um conjunto inconsistente.

Depois, `BOOTSTRAP_IMPORT_WORKERS=2` limita downloads/importadores simultaneos (1–12; o pool Hikari tem 16 conexoes). Cada trabalhador tem
seu temporario, conexao PostgreSQL e DuckDB (128 MB, duas threads) e grava cada arquivo com **`COPY` binario direto na
tabela final** (lotes de 1.000 linhas), dentro de uma unica transacao por arquivo. Cada linha referencia o
arquivo por `partition_id` inteiro, nao pelo URI S3.
A fila de importacao prioriza arquivos por linhas esperadas, em ordem decrescente. Arquivos sem contagem
conhecida (incluindo partes de lugares) vao primeiro; os vazios ficam por ultimo. As partes de lugares nao
tem contagem exata por arquivo no plano: o teste real confere a soma importada contra o snapshot Delta.
A divisao por hash evita repeticao de celulas na importacao, mas cada UNLOAD pode ler a resolucao inteira
no S3: nao e poda de particao. Aumenta o custo de leitura Athena em troca de downloads/COPY menores.
**Carga inicial sem indices (V21).** Somente antes da primeira publicacao, com a tabela vazia ou retomando essa carga, indices, PK e FK sao
registrados em `annual_cache_deferred_index` (com WAL), removidos, e recriados em paralelo antes do `ready`
(PK via `ADD CONSTRAINT ... USING INDEX`; FK validada por uma consulta). Duplicata na PK falha a carga no fim, sem
publicar. Carga parcial sobre cache existente mantem os indices. A importacao comeca durante a exportacao: cada UNLOAD
concluido entra na fila (maiores primeiro, exportacoes grandes primeiro); arquivos de outro snapshot saem antes do
primeiro UNLOAD; o `ready` continua exigindo a reconferencia das versoes Delta. Gravacao por `COPY ... (FORMAT binary)`
com tipos lidos do catalogo; o COPY so abre depois do download. DuckDB com 2 threads por trabalhador.
Medido do zero com 12 trabalhadores: **1 min 34 s** (antes 2 min 17 s); restam ~42 s construindo o PK da grade.
O pool Hikari exige pelo menos trabalhadores+4 conexoes. A falha cancela tarefas e aguarda todos os escritores
terminarem antes de liberar o advisory lock. `ready` so aparece apos todas as publicacoes confirmadas.
A grade exige a particao `month` (`resolution/year/month`, gerada pelo `gold-map`) no log Delta, de onde saem as
linhas esperadas por mes; tabela legada sem ela e recusada (execute `gold-map-repartition`). O UNLOAD filtra o mes
pelo intervalo de `day`, nao por `month`: no Athena, qualquer predicado sobre `month` da `map_grid` falha com
`Failed to generate splits`; o intervalo de dias poda arquivos pelas estatisticas de `day`, entao cada UNLOAD
le so o mes e fica sujeito, isoladamente, ao teto de bytes do workgroup (1 GB por consulta em `fireatlas-api`).
Depois de reparticionar a gold, recrie a tabela no Glue (`aws glue delete-table` + `CREATE EXTERNAL TABLE`);
com o catalogo defasado o Athena recusa toda consulta com `INVALID_GLUE_SCHEMA`.

Permissoes adicionais: `athena:GetWorkGroup`, operacoes de execucao/leitura/cancelamento Athena, leitura Glue/gold,
`s3:ListBucket` e `s3:GetObject|PutObject` nos destinos/resultados, e KMS quando aplicavel. Nao e necessario
`s3:DeleteObject` para carregar; exportacoes obsoletas/orfas precisam de limpeza administrativa ou lifecycle.
Nao expire objetos ainda referenciados por cargas retomaveis. A credencial local permitiu UNLOAD, mas negou
DeleteObject dos probes; esses objetos pequenos permanecem no prefixo de teste.
UNLOAD acrescenta custo de consulta Athena, requisicoes e armazenamento S3; nao ha ganho de tempo garantido.
Referencia: https://docs.aws.amazon.com/athena/latest/ug/unload.html

O Postgres conserva plano, dados e marcadores. Um advisory lock de sessao garante single-flight inclusive entre
instancias da API. Cada arquivo so aparece na grade junto com seu marcador, no commit da sua transacao; uma falha
no meio do arquivo faz rollback e nao deixa linhas parciais. Depois de falha/reinicio, outro POST revalida o
manifesto e reaproveita arquivos confirmados, sem duplicar contagens. GET identifica uma carga interrompida sem
iniciar trabalho. A API publica uma mensagem segura de erro; detalhes completos ficam no log.

O cache usa **PostgreSQL com PostGIS**. A migracao V15 preserva `latitude`/`longitude` publicadas em `h3_places`,
gera `geom geometry(Point, 4326)` e cria `annual_h3_places_geom_gist` para consultas espaciais. As coordenadas
representam o centro da celula H3, nao a posicao de cada incendio. Nao ha conversao de H3 em coordenadas na API:
o banco construi os pontos a partir das coordenadas da gold. A grade diaria referencia esse catalogo por resolucao
e celula, sem repetir geometria para cada dia/sensor. O COPY usa colunas explicitas para que somente o
PostGIS calcule a coluna gerada. Um cache antigo sem coordenadas republica
apenas os arquivos de lugares, preservando a grade anual ja confirmada.

`BOOTSTRAP_REFRESH_HOURS` (padrao **6**, minimo 1) limita a revalidacao de 2026 via POST. Quando vence, o POST
verifica snapshots; exportacoes completas do mesmo snapshot/projecao e arquivos ja publicados sao reutilizados.
Um snapshot novo substitui somente o plano daquele ano. A descoberta da fila historica le o manifesto Delta
mesmo quando 2026 ja esta pronto. GET nao inicia nem atualiza cargas.

**Publicacao por ano (V23).** `annual_grid_bootstrap` conserva estado, progresso e tamanho do plano por ano;
`annual_grid_partition` possui propriedade temporal. Um lock global serializa cargas entre processos/anos e um
marcador por ano identifica o dono ativo. Exportacoes e importacoes continuam paralelas dentro de um unico ano,
com os mesmos limites de 2026. Falha/interrupcao para a fila no ano afetado; o proximo POST retoma os arquivos
confirmados, sem ultrapassar esse ano nem bloquear 2026. Anos prontos, seus indices e os lugares compartilhados
nao sao apagados nem desmontados durante a carga historica.

Mapas, totais diarios/por periodo, ranking, distribuicao/percentis, comparacao/confianca e catalogo de lugares
usam o cache pronto. Consultas de 2026 antes de `ready` retornam 503, sem tentar Athena nem iniciar carga.
Filtros por celulas, pais (ISO a2/a3), continente e cidade/raio continuam na mesma resolucao. Cada ano completo
e servido pelo Postgres; anos pendentes ou incompletos continuam no Athena, sem misturar arquivos parciais.
Periodos mistos leem somente os anos ausentes no Athena em paginas e agregam a uniao completa no Postgres
**antes** de limite/ranking/percentil. Nao ha soma de top-Ns nem percentis parciais. Cobertura e linhas usam
o mesmo snapshot transacional: a publicacao de um ano durante a consulta nao duplica nem perde registros.

### Carga compartilhada de previsoes pelo Athena

`POST /api/v2/bootstrap/forecast` inicia ou retoma a carga em background somente depois que 2026 e **todos os anos
historicos disponiveis no manifesto Delta** estiverem `ready`; antes disso responde `idle` explicando a espera,
sem reivindicar trabalho. `GET /api/v2/bootstrap/forecast` e passivo, sem acesso a AWS, com o mesmo contrato
de progresso do bootstrap anual e `Cache-Control: no-store`. A fila anual inicia a previsao automaticamente
depois do ultimo ano historico; falha ou interrupcao de um ano nao avanca para a previsao.

O plano descobre todos os pares `year/month` ativos do Delta de `gold/forecast`, sem limitar a 2026.
Cada UNLOAD mensal seleciona as colunas nativas e une uma unica linha de lugares por celula r6
(centro, pais, continente e cidade/raio). Nao baixa os Parquets originais nem agrega probabilidades.
**H3 res6 e uma excecao exclusiva das previsoes**: grade de deteccoes, historico e lugares compartilhados
continuam em H3 0–5. Os metadados r6 ficam somente em `forecast_cache`.

A arquitetura reutiliza `AthenaExportStore`, Parquet ZSTD, manifestos validados, DuckDB limitado a 128 MB
e COPY binario em lotes de 1.000 linhas. As exportacoes respeitam os limites `BOOTSTRAP_EXPORT_*`.
Quatro importadores fixos fazem COPY em paralelo. A carga mantem o lock global anual durante todo o lease,
entao previsao e anos nunca disputam o pool de conexoes, inclusive entre instancias da API.
Os comandos e arquivos confirmados sobrevivem a reinicios; uma tentativa retoma o trabalho confirmado.
O tamanho desconhecido de objetos exportados usa o sentinela -1, como no plano anual.

As migracoes V24–V26 criam o cache isolado e permitem exportacoes mensais do tipo `FORECAST`.
Locks proprios impedem cargas de previsao duplicadas; previsao e carga anual sao mutuamente exclusivas.
Arquivos novos ficam em um snapshot de staging: o ponteiro publicado so muda depois da validacao completa.
Falhas de atualizacao preservam a previsao anterior; o POST respeita `BOOTSTRAP_REFRESH_HOURS`.

`/api/v2/forecast` e `/api/v2/forecast/issues` consultam exclusivamente a publicacao PostgreSQL.
Emissoes, lista e resumo da mesma resposta usam um unico snapshot transacional; nenhum resultado parcial
ou probabilidade recalculada e exposto. Sem publicacao inicial valida, a API responde 503.
O provedor opcional legado de `/api/v1`, ativado por `fireatlas.prediction.source=forecast-table`,
mantem seu contrato separado; nao e usado pelo frontend v2.

### Demais dados gold: consulta direta pelo Athena

Os anos ainda nao publicados de `map_grid`, `harmonized_cells` e `harmonized_availability` continuam nas tabelas
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
- `/api/v2` (grade anual): anos publicados e lugares usam PostgreSQL com indices por resolucao/data/celula.
  Somente anos ainda ausentes no cache e outros produtos usam Athena, com poda por particao e reuso de resultados.
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
| gold | `gold/map_grid`, `gold/h3_places`, `gold/forecast`, `gold/harmonized/{cells,availability}` | Cache PostgreSQL para anos publicados/lugares; anos pendentes e outros produtos pelo Athena |
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
  V12 (temperatura), V13 (remove materializacao antiga), **V14 (cache anual compartilhado, staging e progresso)**,
  V15–V17 (PostGIS, exportacoes Athena, exportacao mensal), **V18 (COPY direto sem staging; `partition_id`)**,
  V19 (partes de lugares), **V20 (cache anual `UNLOGGED`)**, **V21 (indices/chaves adiados na carga do zero)**.

Postgres do `docker-compose.yml` ajustado para a carga anual: `shared_buffers=1GB`, `max_wal_size=8GB`,
`checkpoint_timeout=15min`, `wal_compression=on`, `maintenance_work_mem=256MB` e `shm_size: 1g`. Com os padroes
(128 MB de cache, 1 GB de WAL) a carga forcava checkpoints seguidos. Recriar o container mantem o volume.

**Cache sem WAL (V20).** `annual_map_grid`, `annual_h3_places` e `annual_grid_partition` sao `UNLOGGED`: o cache
e reconstruivel a partir do S3, e o WAL era a maior espera da carga (38,6% das sessoes COPY com 12 trabalhadores).
Medido do zero: 3 min 20 s com WAL, **2 min 17 s** sem. Desligamento limpo (`docker compose restart/stop`) preserva
o cache; o `db` tem `stop_grace_period: 2m` para o checkpoint terminar antes de um SIGKILL. Numa queda (SIGKILL,
falta de energia, crash) o Postgres esvazia essas tabelas, dados e marcadores juntos: o status vira
"Cache anual incompleto", consultas de 2026 respondem 503 e o proximo POST recarrega primeiro 2026 e depois
retoma os anos anteriores. `annual_grid_bootstrap` e `annual_athena_export` continuam com WAL: exportacoes
Athena concluidas sao reaproveitadas. As medicoes anteriores de 2026 nao estimam a recarga do historico inteiro.

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
`POST /api/v2/bootstrap/2026` apos o intervalo de revalidacao; o mesmo POST retoma anos historicos pendentes.
Os demais produtos gold continuam nas rotas de consulta existentes.
