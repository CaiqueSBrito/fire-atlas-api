# fire-atlas-api (módulo gold-api)

API de agregados de focos de calor (MODIS e VIIRS) que lê **só a camada ouro** do data lake (Delta Lake), para o NASA
Space Apps 2026. Plataforma aberta, sem login.

> **Este é um programa separado**, em `gold-api/`. Ele **não divide** classes, migrações nem testes com o app principal
> da `main`: tem o próprio `pom.xml`, a própria porta (**8082**) e o próprio esquema no banco (**`gold`**, com histórico
> do Flyway à parte). Os nomes de pacote são os mesmos do projeto final, então juntar depois é mover arquivos.

```
Data lake (S3) ── camada OURO (Delta Lake)
   ├─ gold/map_grid    grade H3 por dia e sensor (0 a 6)         ─┐
   ├─ gold/h3_places   continente, país e cidade por célula H3   ├─ importação (profile "ingest"):
   └─ gold/forecast    previsão de fogo por célula (res. 6)      ─┘  só arquivos ativos do _delta_log
                                   ▼
PostgreSQL (esquema gold) ── daily_h3 · h3_place · forecast · ingestion_log · sensor/satellite
                                   ▼
API REST (/api/v1, porta 8082) ──► Frontend
```

## Rodar localmente

Requisitos: **Java 21**, Maven e Docker.

```bash
docker compose up -d      # PostGIS em localhost:5433
mvn spring-boot:run       # API em http://localhost:8082 (Flyway cria o esquema gold)
```

Documentação interativa: `http://localhost:8082/swagger-ui.html`. **Contrato completo: [`docs/API.md`](docs/API.md).**

## O que a gold permite

| Funciona | Não funciona (sem fonte na gold) |
|---|---|
| Grade no mapa, calendário, série temporal, contagem | Pontos individuais (lat/lon) |
| Ranking e regiões críticas, comparação entre períodos | Harmonização e correspondências MODIS × VIIRS (ficam no app principal, a partir da prata) |
| Comparação com o histórico (a gold tem anos de dados) | Mediana de FRP |
| FRP (soma, média, máximo) e confiança | Filtro por `bbox` |
| **Filtro por continente, país e cidade** (`h3_places`) | |
| **Previsão de fogo por célula** (`forecast`) | |

Toda consulta usa **`cells`** (índices H3 em hexadecimal) **ou** `country`, `continent` ou `city`: a API não converte `bbox` em células.

## Importação da gold

O estado da tabela Delta é reconstruído a partir do último checkpoint e dos commits seguintes do `_delta_log`: **só os arquivos ativos** entram. Credenciais AWS vêm do perfil local (`aws configure`) ou de IAM Role — **nunca** no código ou no `.env`. O acesso ao bucket é somente leitura.

```bash
export DATALAKE_BUCKET=<bucket>       # PowerShell: $env:DATALAKE_BUCKET="<bucket>"
scripts/importar.sh 2023-01-01 2026-12-31 0,1,2,3,4,5      # grade (resoluções opcionais; padrão 0 a 6)
```

Equivale a `java -jar target/fire-atlas-gold-api-0.1.0.jar --spring.profiles.active=ingest --ingest.start=... --ingest.end=... --ingest.resolutions=...`. Para as outras tabelas:

```bash
java -jar target/fire-atlas-gold-api-0.1.0.jar --spring.profiles.active=ingest --ingest.dataset=places     # continente, país e cidade
java -jar target/fire-atlas-gold-api-0.1.0.jar --spring.profiles.active=ingest --ingest.dataset=forecast   # previsão
java -jar target/fire-atlas-gold-api-0.1.0.jar --spring.profiles.active=ingest --ingest.dataset=all --ingest.start=2023-01-01 --ingest.end=2026-12-31
```

`--ingest.dataset` aceita `map_grid` (padrão), `places`, `forecast` ou `all`. A gold completa da grade tem ~5,8 GB (a resolução 6 é a maior): importe o período e as resoluções de que precisa. Para o filtro por **cidade**, a grade da **resolução 5** (ou 6) tem de estar importada.

| Regra | Como é tratada |
|---|---|
| Validação | Cada linha é validada; inválida não é gravada e o motivo é contado em `ingestion_log.rejects` (ex.: `sensor_invalido`, `h3_invalido`, `frp_negativo`, `share_invalido`, `probabilidade_invalida`) |
| Conferência | Arquivo lido por inteiro precisa ter o mesmo número de linhas do log do Delta (`numRecords`); senão a unidade falha (`verified` em `/meta/sources`) |
| Rastreabilidade | Cada linha guarda `source_key` (tabela, partição e versão do Delta); a API devolve `meta.source` |
| Falhas | Fonte indisponível, log incompleto, arquivo corrompido ou sem colunas obrigatórias: **nada** da unidade é gravado, a falha fica registrada (`status = FAILED`) e o processo termina com **código de saída diferente de zero** |
| Idempotência | Cada unidade (partição, resolução ou data de emissão) é uma transação que apaga e recarrega: repetir não duplica |

**Quem alimenta o quê:** o time de dados gera e atualiza `map_grid`, `h3_places` e `forecast` no bucket; este serviço só **importa** para o banco. A `forecast` hoje tem uma emissão por dia (regravada); com que frequência o time atualiza é um combinado a fechar com eles.

## H3

**O backend só consome H3, não converte nada.** Os índices vêm prontos da gold. A API não transforma lat/lon em H3, nem `bbox` em células, nem célula em coordenadas — garantido por teste (`ArchitectureTest`). Cada resolução (0 a 6) já vem agregada: a consulta usa a **resolução das próprias células** informadas. Para país, continente e cidade a consulta **junta a grade com `h3_place` no SQL**, sem lista de células e sem limite de tamanho.

## Variáveis de ambiente

| Variável | Padrão | Para quê |
|---|---|---|
| `DB_URL` | `jdbc:postgresql://localhost:5433/fireatlas?currentSchema=gold` | Conexão com o banco (esquema `gold`) |
| `DB_USER` / `DB_PASSWORD` | `fireatlas` / `fireatlas` | Credenciais do banco (somente dev local) |
| `SERVER_PORT` | `8082` | Porta da API (o app principal usa 8080/8081) |
| `CORS_ORIGINS` | `http://localhost:3000` | Origens do front |
| `DATALAKE_BUCKET` | — | Bucket do data lake (só importação) |
| `GOLD_TABLE` | `gold/map_grid/` | Prefixo da tabela Delta da grade |
| `H3_BASE_RESOLUTION` | `6` | Maior resolução H3 da fonte |
| `AWS_REGION` | `us-east-2` | Região do bucket |

## Testes

```bash
docker compose up -d   # os testes de aceite usam o PostGIS (em esquema temporário, removido ao final)
mvn test               # unitários, leitura do Delta e do Parquet, arquitetura (ArchUnit) e aceite
```

Os testes geram tabelas Delta de verdade (Parquet + `_delta_log`) em pasta local, no formato da gold (`map_grid`, `h3_places` e `forecast`), e importam pelo mesmo caminho da importação real.

## Arquitetura

Pastas por entidade, com as camadas dentro (`controller`, `dto`, `entity`, `mapper`, `policy`, `repository`, `service`; mais `integration` e `cli` na importação), verificadas por ArchUnit: `detection`, `calendar`, `analysis`, `ranking`, `place`, `forecast`, `sensor`, `ingestion`, `region`, `h3` e `common`.
