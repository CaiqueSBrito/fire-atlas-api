# fire-atlas-api

API (Spring Boot) de focos de calor **MODIS e VIIRS** para o NASA Space Apps 2026
("Harmonization of MODIS and VIIRS Hot Spots"). Plataforma aberta, sem login.

```
Data lake (S3, bronze/raw CSV da FIRMS)
        │  ingestao filtrada por regiao + periodo (profile "ingest")
        ▼
PostgreSQL + PostGIS  ── detection (particionada por ano; GiST, BRIN e indice H3 res 8)
        │               ├─ daily_h3 (agregacao diaria por celula H3, resolucoes 3 a 8)
        │               ├─ ingestion_log (proveniencia)
        ▼               └─ user_preference (perfil anonimo via cookie)
API REST (/api/v1) ──► Frontend Next.js
```

## Rodar localmente

Requisitos: **Java 21** (o projeto compila para 21 e foi testado em 21 e 25), Maven, Docker.

```bash
docker compose up -d            # PostGIS em localhost:5433 (evita conflito com outro Postgres na 5432)
mvn spring-boot:run             # API em http://localhost:8080 (Flyway cria o schema)
```

Documentacao interativa: `http://localhost:8080/swagger-ui.html` (OpenAPI em `/v3/api-docs`).

## Importação de dados (fontes, validação e falhas)

**Fonte:** arquivos CSV da NASA FIRMS guardados no data lake (`bronze/raw/<produto>/<ano>/`), cada um com o manifesto
`*.validation.json` publicado junto (linhas esperadas, período observado e sha256 do arquivo original).

**Produtos aprovados (RN01):** `MODIS_C6.1`, `VIIRS_S-NPP`, `VIIRS_NOAA-20` e `VIIRS_NOAA-21` (configurável em
`APPROVED_PRODUCTS`). Qualquer outro produto é recusado. Confirme que esta lista bate com a pesquisa NASA-001.

As credenciais AWS vêm do perfil local (`aws configure`) ou de IAM Role — **nunca** coloque chaves no código ou no `.env`.

```bash
export DATALAKE_BUCKET=<bucket>       # PowerShell: $env:DATALAKE_BUCKET="<bucket>"
scripts/importar.sh 2026-09-01 2026-09-07 -74,-34,-34,6 MODIS,VIIRS       # Linux/macOS/Git Bash
.\scripts\importar.ps1 -Start 2026-09-01 -End 2026-09-07                   # Windows (Brasil por padrão)
```

Equivale a `java -jar target/fire-atlas-api-0.1.0.jar --spring.profiles.active=ingest --ingest.start=... --ingest.end=...
--ingest.bbox=... --ingest.sensors=...`. Opcionais: `--ingest.products=MODIS_C6.1,VIIRS_S-NPP` e, como contingência,
`--ingest.file=amostra.csv --ingest.product=VIIRS_S-NPP` (CSV local baixado previamente).

| Regra | Como é tratada |
|---|---|
| Recorte (RN02) | Só entram linhas dentro da região e do período. Arquivos cujo período (manifesto) não cobre o pedido são **pulados**; se nada cobre, a importação **falha**. |
| Validação (RN03) | Cada linha é validada; inválida não é gravada e o motivo é contado em `ingestion_log.rejects`. |
| Normalização (RN04) | Esquema comum para MODIS e VIIRS; valores originais e unidades preservados (ver tabela abaixo). |
| Rastreabilidade (RN05) | Cada registro guarda sensor, satélite, instrumento, instante UTC, produto e arquivo de origem; a API devolve `product` e `sourceFile`. |
| Falhas (RN06 / CA04) | Fonte indisponível, manifesto ausente, arquivo incompleto (linhas lidas ≠ manifesto) ou produto não aprovado: **nada** do arquivo é gravado, a falha fica registrada (`status = FAILED`, motivo, sem credenciais) e o processo termina com **código de saída ≠ 0**. |

- **Camada ouro (Parquet), uma coluna H3 por resolucao:** com `H3_BASE_RESOLUTION=6` a importacao le a coluna `h3_6` como indice do foco e confere que `h3_0` ... `h3_5` sao os "pais" dela (`h3_inconsistente` se nao forem). **Camada ouro (Parquet):** com `LAKE_PREFIX=gold/`, a importação lê arquivos `.parquet` (um foco por linha, com o H3 já na linha, na mesma estrutura `{produto}/{ano}/` do raw). O Parquet é lido com DuckDB embutido, com **filtro de período e região aplicado na leitura**, e é conferido pelo rodapé do arquivo (arquivo corrompido ou cortado falha). `--ingest.file` também aceita `.parquet`. Os nomes de coluna esperados são os do raw (`latitude`, `longitude`, `acq_date`, `acq_time`, `satellite`...) mais `h3`; confirme com o dado real quando ele chegar.
- Cada CSV é lido em streaming (COPY em lote) e cada arquivo é uma transação: ou entra inteiro ou não entra.
- A rotina é **idempotente** por arquivo e período: repetir a importação não duplica registros.
- Cada execução fica em `ingestion_log` (`GET /api/v1/meta/sources`), com `status`, linhas lidas/gravadas/rejeitadas,
  linhas esperadas, sha256 da fonte e `verified` (arquivo lido por inteiro e conferido com o manifesto).
  Quando a leitura para depois do fim do período (arquivos ordenados), o arquivo não é lido por inteiro e `verified` fica falso.

**Unidades dos valores originais (também nos comentários das colunas do banco):**

| Campo | Unidade / significado |
|---|---|
| `frp` | megawatts (MW) |
| `brightness`, `bright_t31` | kelvin (MODIS: canais 21/22 e 31; VIIRS: bandas I-4 e I-5) |
| `scan`, `track` | km (tamanho do pixel ao longo da varredura e da trilha) |
| `lat`, `lon` | graus decimais WGS84 |
| `acq_ts` | UTC (`acq_date` + `acq_time` originais) |
| `confidence_raw` | original da FIRMS: MODIS 0–100 (%), VIIRS `l`/`n`/`h`; `confidence_class` é a versão harmonizada |
| `instrument`, `fire_type` | originais da FIRMS (`fire_type` só nos arquivos *archive*: 0 vegetação, 1 vulcão, 2 fonte estática, 3 offshore) |

## Endpoints (`/api/v1`)

| Metodo | Rota | O que faz |
|---|---|---|
| GET | `/detections` (`cells` ou `bbox`, `start`, `end`, `sensor`, `zoom`) | Pontos (zoom ≥ 9, `bbox` ou `cells`) ou células H3 agregadas (zoom < 9, **só `cells`**) |
| GET | `/detections/count` (`cells` ou `bbox`, `start`, `end`, `sensor`) | Contagem exata de deteccoes por sensor (com fonte, unidade e metodo) |
| GET | `/calendar` (**`cells`**, `month=YYYY-MM`, `sensor`) | Total por dia/sensor (alimenta as "cartas") |
| GET | `/compare` (`bbox` ou `cells`, `start`, `end`, `sensor`) | Estatisticas por sensor + aviso de nao comparabilidade |
| GET | `/timeseries` (**`cells`**, `start`, `end`, `sensor`) | Serie diaria e dias de atividade incomum |
| GET | `/harmonized` (`bbox` ou `cells`, `start`, `end`) | Harmonizacao MODIS×VIIRS (ver `docs/HARMONIZACAO.md`) |
| GET | `/harmonized/matches` (como `/harmonized` + `limit`) | Camada de correspondencias: pares MODIS×VIIRS com os dois lados e a origem |
| GET | `/compare/periods` (`cells`, `startA`, `endA`, `startB`, `endB`, `sensor`) | Comparacao entre dois periodos, por sensor, com variacao % (so com dados nos dois) |
| GET | `/compare/historical` (`cells`, `start`, `end`, `sensor`, `years`) | Periodo contra as mesmas datas de anos anteriores: media, z-score e anomalia (so com 3+ anos) |
| GET | `/rankings/cells` (`cells`, `start`, `end`, `metric`, `resolution`, `limit`, `criticalPercentile`) | Ranking de celulas H3 e regioes criticas |
| GET/PUT | `/preferences` | Regiao favorita do usuario anonimo (cookie `fa_uid`) |
| GET | `/meta/sources`, `/meta/coverage`, `/meta/sensors` | Rastreabilidade, cobertura e catalogo de sensores/satelites |

Região: informe **`bbox`** (`minLon,minLat,maxLon,maxLat`) **ou `cells`** (índices H3 em hexadecimal, separados por vírgula, resolução até 8), nunca os dois. `sensor` = `MODIS | VIIRS | BOTH`. Sem resultados, a resposta traz `message`.
Erros de validacao retornam `400` (RFC 7807) com a lista `errors` de parametros rejeitados. **Contrato completo: [`docs/API.md`](docs/API.md).**

## Performance

- Tabela particionada por ano + indice GiST (geometria) + BRIN (data): toda consulta usa bbox + datas.
- Zoom afastado, calendario e serie leem `daily_h3` (pre-agregada por celula H3), nunca as deteccoes brutas.
- Payload limitado (5000 pontos / 20000 celulas, com flag `truncated`), gzip e `Cache-Control`.

## Cookies / perfil anonimo

O front deve chamar a API com `credentials: "include"`. Se front e API estiverem em dominios diferentes (HTTPS),
use `COOKIE_SAME_SITE=None` e `COOKIE_SECURE=true`, e inclua o dominio do front em `CORS_ORIGINS`.

## Variáveis de ambiente (RN01)

Todas têm padrão para desenvolvimento local; só `DATALAKE_BUCKET` é obrigatória, e apenas para a ingestão.
Use `.env.example` como modelo (o Spring não lê `.env` sozinho: exporte as variáveis ou configure na sua IDE).

| Variável | Padrão | Para quê |
|---|---|---|
| `DB_URL` | `jdbc:postgresql://localhost:5433/fireatlas` | Conexão com o PostGIS |
| `DB_USER` / `DB_PASSWORD` | `fireatlas` / `fireatlas` | Credenciais do banco (somente dev local) |
| `CORS_ORIGINS` | `http://localhost:3000` | Origens do front, separadas por vírgula |
| `COOKIE_SAME_SITE` / `COOKIE_SECURE` | `Lax` / `false` | Cookie do perfil anônimo (`None`/`true` com front em outro domínio HTTPS) |
| `DATALAKE_BUCKET` | — | Bucket do data lake (só ingestão) |
| `APPROVED_PRODUCTS` | `MODIS_C6.1,VIIRS_S-NPP,VIIRS_NOAA-20,VIIRS_NOAA-21` | Produtos aprovados para importação (RN01) |
| `H3_COLUMN` | `h3` | Coluna da fonte com o índice H3 pronto (hexadecimal ou inteiro de 64 bits); o backend só a consome |
| `H3_BASE_RESOLUTION` | `8` | Resolucao do H3 guardado em cada foco (a mais fina que a fonte entrega): 8 no dado bruto; 6 no ouro (que entrega de 0 a 6, uma coluna `h3_0` ... `h3_6` por resolucao) |
| `LAKE_PREFIX` | `bronze/raw/` | Pasta do data lake lida pela importação: `bronze/raw/` (CSV bruto, sem H3) ou `gold/` (Parquet da camada ouro, com H3) |
| `AWS_REGION` | `us-east-2` | Região do bucket |
| `TEST_DB_URL` / `TEST_DB_USER` / `TEST_DB_PASSWORD` | igual ao dev local | Banco usado pelos testes de aceite |

Credenciais AWS **não** são variáveis do projeto: vêm do perfil local (`aws configure`) ou de IAM Role.

## Arquitetura (RN02): pastas por entidade, com as camadas dentro de cada uma

Cada **entidade** do negócio tem a sua pasta e, dentro dela, as **camadas** (`controller`, `dto`, `entity`, `mapper`,
`policy`, `repository`, `service`; mais `integration` e `cli` na importação). Só existem as camadas de que a entidade precisa.

```
com.fireatlas
 ├─ detection/       controller · dto · entity · mapper · repository · service   focos: pontos, células H3, cobertura
 ├─ calendar/        controller · dto · mapper · service                          calendário (cartas por dia)
 ├─ analysis/        controller · dto · policy · service                          comparação e série temporal (z-score)
 ├─ harmonization/   controller · dto · entity · repository · service             pareamento MODIS × VIIRS
 ├─ sensor/          controller · dto · entity · mapper · policy · repository · service   catálogo de sensores e satélites
 ├─ preference/      controller · entity · repository · service                   perfil anônimo (cookie)
 ├─ ingestion/       cli · controller · entity · integration · repository · service       importação do data lake
 ├─ region/          entity · repository                                          BBox, Region e filtros SQL de região
 ├─ h3/              policy                                                       H3Index (só consome H3)
 ├─ common/          controller · policy                                          QueryParams, cache HTTP, erros, mensagens
 └─ config/                                                                       CORS
```

| Camada (subpasta) | Responsabilidade | Não pode |
|---|---|---|
| `controller` / `cli` | Entradas: REST e linha de comando. Leem a requisição (parâmetros, cookie, cache) e chamam o serviço | acessar repositório ou SQL |
| `service` | Casos de uso e regras de negócio | conhecer HTTP, SQL ou AWS |
| `mapper` | Converte entidade em DTO | ter regra de negócio |
| `dto` | Contratos de resposta da API | depender de camadas acima |
| `repository` | Persistência (SQL/PostGIS); devolve **entidades**, nunca DTO | conhecer `controller`, `dto` ou `service` |
| `integration` | Fonte externa: data lake (S3), leitura de CSV e de Parquet | conhecer `service` ou `controller` |
| `entity` / `policy` | Entidades, objetos de valor e regras puras | usar Spring, JDBC ou servlet |

Dentro de uma entidade as dependências vão sempre **de cima para baixo** (`controller/cli → service → repository/integration → entity`).
Essas regras são **verificadas por teste** (`ArchitectureTest`, ArchUnit): uma dependência na direção errada, um arquivo solto
fora de uma camada, SQL fora da persistência ou qualquer conversão de coordenadas em H3 quebram o build.

O modelo de dados está em `src/main/resources/db/migration` (Flyway): `detection` (medições originais com sensor,
satélite, instrumento, localização e origem), `daily_h3` (agregação por célula H3), `ingestion_log` (metadados, proveniência e falhas de cada importação),
`harmonization_run` (resultados derivados) e `user_preference`.

## Integridade, proveniência e dados originais (RN03–RN05)

- **Validação na entrada (RN03/CA03):** cada linha é validada (latitude/longitude, data, hora, satélite, FRP ≥ 0 e campos
  numéricos). Linha inválida **não é gravada** e entra na contagem por motivo (`latitude_invalida`, `longitude_invalida`,
  `data_invalida`, `hora_invalida`, `satelite_ausente`, `frp_invalido`, `campo_numerico_invalido`), registrada em log e em
  `ingestion_log.rejects`. O banco repete as regras com `CHECK` como defesa em profundidade.
- **Proveniência (RN04):** toda detecção guarda `sensor`, `satellite`, `source_product` e `source_file` (chave do objeto no lake).
- **Originais imutáveis (RN05/CA04):** `detection` não aceita `UPDATE` (gatilho no banco). Resultados derivados vão para
  `harmonization_run`, sem tocar nas medições. Correções entram por nova carga do arquivo (idempotente).

## Erros (RN06)

Erros de validação respondem `400` (RFC 7807). Falha de banco responde `503` com mensagem genérica; o detalhe técnico vai
só para o log, sem URL, usuário ou senha de conexão. Erros inesperados respondem `500` genérico.

## Testes

```bash
docker compose up -d   # os testes de aceite usam o PostGIS (em schema temporário, removido ao final)
mvn test               # 55 testes: 17 unitários, 4 de mappers, 6 de leitura de Parquet, 10 de arquitetura (ArchUnit) e 18 de aceite (CA01 a CA04 dos dois cards); sem o banco, os de aceite são pulados
```

## H3 (indexação geográfica)

**O backend só consome H3, não converte nada.** O índice H3 de cada detecção vem **pronto do S3** (o pipeline de dados
converte as coordenadas antes). A API não transforma lat/lon em H3, nem `bbox` em células, nem célula em coordenadas.
Isso é garantido por teste (`ArchitectureTest`: nenhum código pode chamar `latLngToCell`, `cellToLatLng`, `polygonToCells`...).

| Onde | Como |
|---|---|
| Cada detecção | guarda o índice H3 (**resolução 8 ou mais fina**) lido da coluna da fonte (`H3_COLUMN`, padrão `h3`; hexadecimal ou inteiro de 64 bits); índice inválido ou de resolução menor que 8 rejeita o registro (`h3_invalido`); sem a coluna, fica ausente |
| Mapa afastado (`zoom` < 9) | **exige `cells`**; devolve **células H3** (`h3` em hexadecimal, `resolution`, `count`, FRP), sem coordenadas; resolução 3 a 7 conforme o zoom |
| Calendário e série temporal | **exigem `cells`**; somam as células H3 informadas, em `daily_h3` |
| Pontos (`zoom` ≥ 9), comparação, harmonização | aceitam `bbox` (filtro exato do PostGIS) **ou** `cells` (índice `h3`) |
| Perfil (`/preferences`) | guarda `bbox` e/ou `cells` |

- As resoluções 3 a 7 são **ancestrais** do índice (relação hierárquica, calculada com operações de bits em SQL,
  `H3Index.parentSql`, e testada contra a biblioteca H3).
- Pedir consulta agregada com `bbox` responde `400` com a mensagem
  "esta consulta agregada so aceita cells (indices H3): a API nao converte bbox em celulas".
- Limites: até 2000 células em `cells`; a resolução é reduzida automaticamente até caber (20 mil células no mapa, 5 mil em
  calendário/série); o campo `resolution` da resposta informa a resolução usada.
- **Registros sem H3** não aparecem no mapa afastado, no calendário nem na série (que usam `daily_h3`), nem nas consultas
  por `cells`; os pontos por `bbox` continuam funcionando. Os CSVs brutos atuais do lake (`bronze/raw`) **não** trazem a
  coluna `h3`; ela virá do dado já convertido no S3. Reimportar depois é seguro (idempotente por arquivo e período).

**Para o front:** para chamar o mapa afastado, o calendário e a série, o front precisa mandar `cells`; com `h3-js`, obtém as
células da área visível (`polygonToCells`) e desenha as células devolvidas pelo próprio índice H3 (`H3HexagonLayer`).
Pontos individuais, comparação e harmonização não exigem H3.

## Qualidade dos dados observada

O data lake guarda o dado **bruto** da FIRMS (sem normalização) e do mundo inteiro, por isso os arquivos têm milhões de linhas.
A FIRMS publica algumas detecções VIIRS com **FRP negativo** (ex.: −0,66, −1, −2,57; no VIIRS S-NPP NRT 2026: 6 em 6,3 milhões,
todas diurnas e de baixa confiança). **Decisão da equipe: FRP negativo não entra.** A validação rejeita essas linhas
(`frp_invalido`, contadas em `ingestion_log.rejects`) e o banco recusa FRP < 0 (`CHECK`).

## Modelo de dados e entidades

| Tabela | Entidade Java | Papel |
|---|---|---|
| `sensor` | `Sensor` | Catalogo: MODIS (1000 m) e VIIRS (375 m), com nome e descricao; a resolucao nominal vem daqui |
| `satellite` | `Satellite` | Plataformas que carregam cada sensor: Terra e Aqua (MODIS); SNPP, N20 e N21 (VIIRS) |
| `detection` | `Detection` | Medicao original (sensor, satelite, instante UTC, FRP, confianca...) com `Location` (lat/lon + H3) e `Source` (produto e arquivo do lake) |
| `daily_h3` | — | Agregacao diaria por celula H3 e sensor (derivada) |
| `ingestion_log` | — | Metadados de cada carga: origem, regiao, periodo, linhas lidas/gravadas/rejeitadas por motivo |
| `harmonization_run` | — | Resultados derivados (nao alteram `detection`) |
| `user_preference` | — | Regiao favorita do usuario anonimo |

`detection.sensor` tem chave estrangeira para `sensor`: uma deteccao so existe para um sensor do catalogo. O parser da
ingestao produz a entidade `Detection` (invariantes validadas no construtor) e o carregador a grava via `COPY`.
