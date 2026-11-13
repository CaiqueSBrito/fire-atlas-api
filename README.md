# fire-atlas-api

API (Spring Boot) de focos de calor **MODIS e VIIRS** para o NASA Space Apps 2026
("Harmonization of MODIS and VIIRS Hot Spots"). Plataforma aberta, sem login.

```
Data lake (S3, bronze/raw CSV da FIRMS)
        │  ingestao filtrada por regiao + periodo (profile "ingest")
        ▼
PostgreSQL + PostGIS  ── detection (particionada por ano, GiST + BRIN)
        │               ├─ daily_grid (grade 0,1°/dia pre-agregada)
        │               ├─ ingestion_log (proveniencia)
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

```bash
export DATALAKE_BUCKET=<bucket>   # PowerShell: $env:DATALAKE_BUCKET="<bucket>"
mvn spring-boot:run -Dspring-boot.run.profiles=ingest \
  -Dspring-boot.run.arguments="--ingest.start=2024-08-01 --ingest.end=2024-09-30 --ingest.bbox=-74,-34,-34,6 --ingest.sensors=MODIS,VIIRS"
```

- Cada CSV anual e lido em streaming e so entram as linhas dentro da regiao/periodo (COPY em lote).
- Cargas repetidas sao idempotentes por arquivo e periodo.
- `--ingest.products=MODIS_C6.1,VIIRS_S-NPP` restringe os produtos.
- Contingencia (CSV local): `--ingest.file=amostra.csv --ingest.product=VIIRS_S-NPP`.
- Cada carga registra arquivo de origem, regiao, periodo e contagens em `ingestion_log` (`GET /api/v1/meta/sources`).

## Endpoints (`/api/v1`)

| Metodo | Rota | O que faz |
|---|---|---|
| GET | `/detections?bbox&start&end&sensor&zoom` | Pontos (zoom ≥ 9) ou celulas agregadas (zoom < 9) |
| GET | `/calendar?bbox&month=YYYY-MM&sensor` | Total por dia/sensor (alimenta as "cartas") |
| GET | `/compare?bbox&start&end&sensor` | Estatisticas por sensor + aviso de nao comparabilidade |
| GET | `/timeseries?bbox&start&end&sensor` | Serie diaria e dias de atividade incomum |
| GET | `/harmonized?bbox&start&end` | Harmonizacao MODIS×VIIRS (ver `docs/HARMONIZACAO.md`) |
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
| `AWS_REGION` | `us-east-2` | Região do bucket |
| `TEST_DB_URL` / `TEST_DB_USER` / `TEST_DB_PASSWORD` | igual ao dev local | Banco usado pelos testes de aceite |

Credenciais AWS **não** são variáveis do projeto: vêm do perfil local (`aws configure`) ou de IAM Role.

## Módulos (RN02)

| Pacote | Responsabilidade |
|---|---|
| `ingestion` | Integração com o data lake: leitura do CSV, validação (RN03), carga em lote |
| `domain` | Tipos de domínio puros (`BBox`, grade geográfica) |
| `query` | Persistência e consultas ao PostGIS; análises (z-score robusto); gravação de resultados derivados |
| `api` | Controllers REST, DTOs, validação de parâmetros e tratamento de erros |
| `config` | CORS e configuração web |

O modelo de dados está em `src/main/resources/db/migration` (Flyway): `detection` (medições originais com sensor,
satélite, instrumento, localização e origem), `daily_grid` (agregação), `ingestion_log` (metadados/proveniência da carga),
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
mvn test               # 8 unitários + 4 de aceite (CA01 a CA04); sem o banco, os de aceite são pulados
```
