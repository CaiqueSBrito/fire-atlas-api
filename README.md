# fire-atlas-api

API (Spring Boot) de focos de calor **MODIS e VIIRS** para o NASA Space Apps 2026
("Harmonization of MODIS and VIIRS Hot Spots"). Plataforma aberta, sem login.

```
Data lake (S3) ── camada OURO: tabela Delta gold/map_grid
        │          (uma linha por celula H3, dia e sensor; H3 de 0 a 6; ja agregada)
        │  importacao (profile "ingest"): le o _delta_log, so arquivos ativos
        ▼
PostgreSQL + PostGIS ── daily_h3 (grade H3 diaria por sensor, com proveniencia)
        │               ├─ ingestion_log (metadados e falhas de cada importacao)
        ▼               └─ user_preference (perfil anonimo via cookie)
API REST (/api/v1) ──► Frontend Next.js
```

**Fonte unica: a camada ouro.** A API nao usa a camada bronze (CSV bruto) nem guarda focos individuais: a gold traz so
agregados por celula H3, dia e sensor. Veja *O que a gold permite* abaixo.

## Rodar localmente

Requisitos: **Java 21** (o projeto compila para 21 e foi testado em 21 e 25), Maven, Docker.

```bash
docker compose up -d            # PostGIS em localhost:5433 (evita conflito com outro Postgres na 5432)
mvn spring-boot:run             # API em http://localhost:8080 (Flyway cria o schema)
```

Documentacao interativa: `http://localhost:8080/swagger-ui.html` (OpenAPI em `/v3/api-docs`).

## O que a gold permite

A tabela `gold/map_grid` (Delta Lake, particionada por `resolution` 0 a 6 e `year`, desde 2000) traz por linha:
`h3_cell`, `day`, `sensor`, `detections`, `platforms`, `frp_sum_mw`, `frp_mean_mw`, `frp_max_mw`, `brightness_max_k`,
`viirs_confidence_low/nominal/high` e `modis_confidence_mean`.

| Funciona | Nao funciona (sem fonte) |
|---|---|
| Grade no mapa, calendario, serie temporal, contagem | Pontos individuais (lat/lon) |
| Ranking e regioes criticas, comparacao entre periodos | Harmonizacao e correspondencias MODIS × VIIRS (`/harmonized*` responde **501**) |
| Comparacao com o historico (a gold tem anos de dados) | Mediana de FRP |
| FRP (soma, media, maximo) e confianca | Filtro por continente/pais/cidade e por `bbox` |

Toda consulta usa **`cells`** (indices H3 em hexadecimal): a API nao converte `bbox` em celulas.

## Importacao da gold

O estado da tabela Delta e reconstruido a partir do ultimo checkpoint e dos commits seguintes do `_delta_log`:
**so os arquivos ativos** entram (arquivos removidos ou antigos sao ignorados). Credenciais AWS vem do perfil local
(`aws configure`) ou de IAM Role — **nunca** no codigo ou no `.env`. O acesso ao bucket e somente leitura.

```bash
export DATALAKE_BUCKET=<bucket>       # PowerShell: $env:DATALAKE_BUCKET="<bucket>"
scripts/importar.sh 2023-01-01 2026-12-31 0,1,2,3,4,5      # Linux/macOS/Git Bash (resolucoes opcionais; padrao 0 a 6)
.\scripts\importar.ps1 -Start 2023-01-01 -End 2026-12-31 -Resolutions 0,1,2,3,4,5   # Windows
```

Equivale a `java -jar target/fire-atlas-api-0.1.0.jar --spring.profiles.active=ingest --ingest.start=... --ingest.end=...
--ingest.resolutions=...`. A gold completa tem ~5,8 GB (a resolucao 6 e a maior, ~2,5 GB): importe o periodo e as
resolucoes de que precisa. As consultas usam a resolucao das proprias celulas informadas.

| Regra | Como e tratada |
|---|---|
| Recorte | So entram os dias do periodo pedido e as resolucoes escolhidas; se nenhuma particao cobre o pedido, a importacao **falha** |
| Validacao | Cada linha e validada; invalida nao e gravada e o motivo e contado em `ingestion_log.rejects`: `data_ausente`, `sensor_invalido` (so MODIS e VIIRS), `h3_invalido` (ausente ou de outra resolucao), `deteccoes_invalidas`, `frp_negativo` (decisao da equipe) |
| Valores originais | Soma, media e maximo de FRP, brilho e confianca ficam como a fonte entrega; unidades nos comentarios das colunas |
| Rastreabilidade | Cada linha guarda `source_key` (`gold/map_grid/resolution=R/year=A@vVERSAO`); a API devolve `meta.source` |
| Conferencia | Arquivo lido por inteiro precisa ter o mesmo numero de linhas do log do Delta (`numRecords`); senao a particao falha (`verified` em `/meta/sources`) |
| Falhas | Fonte indisponivel, log incompleto, arquivo corrompido ou sem colunas obrigatorias: **nada** da particao e gravado, a falha fica registrada (`status = FAILED`, sem credenciais) e o processo termina com **codigo de saida diferente de zero** |
| Idempotencia | Cada particao (resolucao e ano) e uma transacao que apaga e recarrega o intervalo: repetir nao duplica |

## Endpoints (`/api/v1`)

| Metodo | Rota | O que faz |
|---|---|---|
| GET | `/detections` (`cells`, `start`, `end`, `sensor`, `zoom`) | Celulas H3 agregadas na resolucao do zoom (0 a 6) |
| GET | `/detections/count` (`cells`, `start`, `end`, `sensor`) | Contagem de deteccoes por sensor (com fonte, unidade e metodo) |
| GET | `/calendar` (`cells`, `month=YYYY-MM`, `sensor`) | Total por dia/sensor (alimenta as "cartas") |
| GET | `/compare` (`cells`, `start`, `end`, `sensor`) | Estatisticas por sensor + aviso de nao comparabilidade |
| GET | `/compare/periods` (`cells`, `startA`, `endA`, `startB`, `endB`, `sensor`) | Dois periodos, com variacao % (so com dados nos dois) |
| GET | `/compare/historical` (`cells`, `start`, `end`, `sensor`, `years`) | Periodo contra as mesmas datas de anos anteriores: media, z-score e anomalia (so com 3+ anos) |
| GET | `/timeseries` (`cells`, `start`, `end`, `sensor`) | Serie diaria e dias de atividade incomum |
| GET | `/rankings/cells` (`cells`, `start`, `end`, `metric`, `resolution`, `limit`, `criticalPercentile`) | Ranking de celulas H3 e regioes criticas |
| GET | `/harmonized`, `/harmonized/matches` | **501**: dependem de focos individuais, que a gold nao tem |
| GET/PUT | `/preferences` | Regiao favorita do usuario anonimo (cookie `fa_uid`) |
| GET | `/meta/sources`, `/meta/coverage`, `/meta/sensors` | Importacoes, cobertura e catalogo de sensores/satelites |

`sensor` = `MODIS | VIIRS | BOTH`. Sem resultados, a resposta traz `message`. Erros de validacao retornam `400` (RFC 7807)
com a lista `errors` de parametros rejeitados. **Contrato completo: [`docs/API.md`](docs/API.md).**

## H3 (indexacao geografica)

**O backend so consome H3, nao converte nada.** O indice H3 de cada celula vem **pronto da gold**. A API nao transforma
lat/lon em H3, nem `bbox` em celulas, nem celula em coordenadas — garantido por teste (`ArchitectureTest`).

- Cada resolucao (0 a 6) ja vem agregada na gold: a consulta usa a **resolucao das proprias celulas** informadas, sem
  expandi-las. Celulas mais finas que 6 sobem para o ancestral na resolucao 6 (operacao de bits sobre o indice).
- O mapa devolve celulas (`h3` em hexadecimal, `resolution`, `count`, FRP), sem coordenadas; quem desenha usa o proprio H3.
- Pedir uma consulta com `bbox` responde `400`: "esta consulta agregada so aceita cells (indices H3): a API nao converte
  bbox em celulas".
- Limites: ate 2000 celulas em `cells`; a resolucao e reduzida ate caber (20 mil celulas no mapa, 5 mil em calendario/serie).

**Para o front:** com `h3-js`, obtenha as celulas da area visivel (`polygonToCells`) e envie em `cells`; desenhe as celulas
devolvidas com o proprio indice (`H3HexagonLayer`).

## Cookies / perfil anonimo

O front deve chamar a API com `credentials: "include"`. Se front e API estiverem em dominios diferentes (HTTPS),
use `COOKIE_SAME_SITE=None` e `COOKIE_SECURE=true`, e inclua o dominio do front em `CORS_ORIGINS`.

## Variaveis de ambiente

Todas tem padrao para desenvolvimento local; so `DATALAKE_BUCKET` e obrigatoria, e apenas para a importacao.
Use `.env.example` como modelo (o Spring nao le `.env` sozinho: exporte as variaveis ou configure na sua IDE).

| Variavel | Padrao | Para que |
|---|---|---|
| `DB_URL` | `jdbc:postgresql://localhost:5433/fireatlas` | Conexao com o PostGIS |
| `DB_USER` / `DB_PASSWORD` | `fireatlas` / `fireatlas` | Credenciais do banco (somente dev local) |
| `CORS_ORIGINS` | `http://localhost:3000` | Origens do front, separadas por virgula |
| `COOKIE_SAME_SITE` / `COOKIE_SECURE` | `Lax` / `false` | Cookie do perfil anonimo |
| `DATALAKE_BUCKET` | — | Bucket do data lake (so importacao) |
| `GOLD_TABLE` | `gold/map_grid/` | Prefixo da tabela Delta da camada ouro |
| `H3_BASE_RESOLUTION` | `6` | Maior resolucao H3 da fonte (a gold entrega de 0 a 6) |
| `AWS_REGION` | `us-east-2` | Regiao do bucket |
| `TEST_DB_URL` / `TEST_DB_USER` / `TEST_DB_PASSWORD` | igual ao dev local | Banco usado pelos testes de aceite |

Credenciais AWS **nao** sao variaveis do projeto: vem do perfil local (`aws configure`) ou de IAM Role.

## Arquitetura: pastas por entidade, com as camadas dentro de cada uma

Cada **entidade** do negocio tem a sua pasta e, dentro dela, as **camadas** (`controller`, `dto`, `entity`, `mapper`,
`policy`, `repository`, `service`; mais `integration` e `cli` na importacao).

```
com.fireatlas
 ├─ detection/       controller · dto · entity · mapper · repository · service   grade H3, contagem, cobertura
 ├─ calendar/        controller · dto · mapper · service                          calendario (cartas por dia)
 ├─ analysis/        controller · dto · policy · service                          comparacoes, historico e serie temporal
 ├─ ranking/         controller · dto · mapper · service                          ranking e regioes criticas
 ├─ harmonization/   controller · service                                         501 (sem focos individuais na gold)
 ├─ sensor/          controller · dto · entity · mapper · policy · repository · service   catalogo de sensores
 ├─ preference/      controller · entity · repository · service                   perfil anonimo (cookie)
 ├─ ingestion/       cli · controller · entity · integration · repository · service       importacao da gold
 ├─ region/          entity · repository                                          Region (celulas H3) e filtros SQL
 ├─ h3/              policy                                                       H3Index (so consome H3)
 ├─ common/          controller · dto · policy                                    QueryParams, cache HTTP, erros
 └─ config/                                                                       CORS
```

Dentro de uma entidade as dependencias vao **de cima para baixo** (`controller/cli → service → repository/integration →
entity`), verificado por `ArchitectureTest` (ArchUnit): dependencia na direcao errada, arquivo solto fora de uma camada,
SQL fora da persistencia ou conversao de coordenadas em H3 quebram o build.

O modelo de dados esta em `src/main/resources/db/migration` (Flyway): `daily_h3` (grade por celula H3, dia e sensor),
`sensor`/`satellite` (catalogo), `ingestion_log` (metadados, proveniencia e falhas de cada importacao) e
`user_preference`. A tabela de focos individuais (`detection`, bronze) foi removida na migracao V6.

## Erros

Erros de validacao respondem `400` (RFC 7807). Recurso sem fonte de dados responde `501`. Falha de banco responde `503`
com mensagem generica (o detalhe tecnico vai so para o log, sem URL, usuario ou senha). Erros inesperados: `500` generico.

## Testes

```bash
docker compose up -d   # os testes de aceite usam o PostGIS (em schema temporario, removido ao final)
mvn test               # unitarios, mappers, leitura do Delta e do Parquet, arquitetura (ArchUnit) e aceite; sem o banco, o aceite e pulado
```

Os testes geram uma tabela Delta de verdade (Parquet + `_delta_log`) em pasta local, no formato da gold, e importam
pelo mesmo caminho da importacao real.

## Qualidade dos dados

A gold ja vem agregada pelo pipeline da equipe de dados. Na importacao, linhas com sensor fora de MODIS/VIIRS,
H3 de outra resolucao, zero deteccoes ou **FRP negativo** (decisao da equipe) sao rejeitadas e contadas por motivo.
