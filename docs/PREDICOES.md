# Previsao de Focos de Calor — Diagnostico, Arquitetura de Integracao e Contrato (NASA-020)

> **Estado padrao atual:** `MODEL_NOT_VALIDATED` (`available = false` em `GET /api/v1/predictions`; `fireatlas.prediction.enabled = false`; **nao existe modelo preditivo validado neste repositorio**).

---

## 1. Escopo Condicional e Visao Geral da NASA-020

A NASA-020 estabelece:

1. **Diagnostico cientifico e bloqueio padrao (`RN01`–`RN05`, `CA01`–`CA03`):** enquanto nao existir modelo preditivo aprovado e avaliado fora da amostra, a API responde explicitamente com indisponibilidade estruturada (`MODEL_NOT_VALIDATED`), sem inventar coordenadas, probabilidades, confianca ou FRP futuras.
2. **Infraestrutura real de integracao desacoplada (Gate B.1):** o backend contem a porta hexagonal (`PredictionProvider`), o provedor desativado padrao (`DisabledPredictionProvider`), o adaptador HTTP real para o protocolo v1 (`HttpPredictionProvider`) e a politica de manifesto e bloqueio cientifico na inicializacao (`PredictionModelProperties` + `PredictionModelConfig`).

Conforme `.agents/references/00-FONTES_E_ESCOPO.md`, `01-ARQUITETURA_E_CONTRATOS.md` e `03-PLANO_DE_ENTREGA.md`, a frente preditiva e complementar e **nao bloqueia** ingestao FIRMS, mapa (`/detections`), calendario (`/calendar`), comparacao (`/compare`), serie temporal (`/timeseries`) ou harmonizacao exploratoria (`/harmonized`, `NASA-018`).

---

## 2. Estado Atual do Modelo (Diagnostico)

A auditoria integral do repositorio confirmou que:

- **Nao ha artefato de modelo** (`.pkl`, `.onnx`, `.pt`, `.h5`, `.pmml`), script Python de inferencia/treinamento ou biblioteca de machine learning em `pom.xml`.
- **Nao ha variavel-alvo formalmente aprovada**, horizonte preditivo homologado nem relatorio de avaliacao fora da amostra.
- **`Anomaly.robustZ` (`/api/v1/timeseries`) nao e modelo preditivo:** calcula apenas z-score robusto descritivo (mediana/MAD, `THRESHOLD = 3.5`) sobre contagens diarias ja observadas dentro da propria janela consultada (incluindo datas posteriores a cada dia dentro de `[start, end]`, o que causaria vazamento temporal se tratado como previsao).
- **`/api/v1/harmonized` (`METHOD_NOT_APPROVED`)** realiza apenas associacao espacial/temporal exploratoria entre deteccoes passadas MODIS e VIIRS.

---

## 3. Arquitetura de Integracao e Responsabilidade de Cada Componente

A integracao segue arquitetura hexagonal pragmatica, separando estritamente a API publica do protocolo de inferencia:

```
Cliente / Frontend
       |
       | GET /api/v1/predictions?bbox&start&end
       v
+----------------------------+
| PredictionController       |  Camada HTTP publica (/api/v1), validacao QueryParams/BBox
+----------------------------+
       |
       v
+----------------------------+
| PredictionService          |  Orquestracao de aplicacao, geracao de requestId e cutoff UTC (asOf)
+----------------------------+
       |
       v
+----------------------------+
| <<interface>>              |  Porta de integracao independente de URL, HTTP ou tecnologia ML
| PredictionProvider         |
+----------------------------+
       ^
       |---------------------------------------------+
       | (enabled = false, padrao)                   | (enabled = true + manifesto valido)
+-------------------------------+         +-------------------------------+
| DisabledPredictionProvider    |         | HttpPredictionProvider        |
| Retorna MODEL_NOT_VALIDATED   |         | Cliente HTTP v1 + validacoes  |
| Zero chamadas HTTP / Zero DB  |         | de escopo, cutoff e contrato  |
+-------------------------------+         +-------------------------------+
```

### Responsabilidades

| Componente | Pacote / Arquivo | Responsabilidade |
|---|---|---|
| `PredictionController` | `com.fireatlas.api.PredictionController` | Valida `bbox`, `start`, `end` e teto operacional (`max-days-predictions: 31`) e serializa `Dto.PredictionResponse` (`Cache-Control: no-store`). |
| `PredictionService` | `com.fireatlas.query.PredictionService` | Determina o instante de corte UTC (`asOf` via `Clock`), gera `requestId`, invoca a porta `PredictionProvider` e converte `ProviderResult` para `Dto.PredictionResponse`. Nao conhece URL, cliente HTTP nem banco. |
| `PredictionProvider` | `com.fireatlas.prediction.PredictionProvider` | Porta de integracao com tipos imutaveis `ProviderRequest`, `ProviderResult` e `OutcomeStatus` (`MODEL_NOT_VALIDATED`, `OUT_OF_APPROVED_SCOPE`, `INSUFFICIENT_DATA_COVERAGE`, `AVAILABLE`). |
| `DisabledPredictionProvider` | `com.fireatlas.prediction.DisabledPredictionProvider` | Implementacao padrao quando `fireatlas.prediction.enabled=false`. Retorna `MODEL_NOT_VALIDATED` sem realizar chamadas de rede ou banco. |
| `HttpPredictionProvider` | `com.fireatlas.prediction.HttpPredictionProvider` | Adaptador HTTP v1 baseado em `java.net.http.HttpClient`. Verifica escopo espacial/temporal antes da chamada, aplica timeouts e limite de bytes, valida estritamente o payload remoto e lanca `PredictionProviderException` em falhas operacionais ou violacoes de contrato. |
| `PredictionModelProperties` / `PredictionModelConfig` | `com.fireatlas.prediction.*` | Carrega `fireatlas.prediction.*` e exige manifesto cientifico completo na inicializacao caso `enabled=true`, falhando de forma controlada (`IllegalStateException`) se faltar qualquer pre-requisito. |
| `ApiExceptionHandler` | `com.fireatlas.api.ApiExceptionHandler` | Traduz `PredictionProviderException` para HTTP `503 Service Unavailable` (`ProblemDetail` RFC 7807) com `status` (`PROVIDER_UNAVAILABLE` ou `INVALID_PROVIDER_RESPONSE`) e `available=false`, sem vazar URLs internas, IPs ou stack traces. |

---

## 4. Especificacao HTTP do Contrato de Inferencia (`protocolVersion = "v1"`)

Quando um servico de inferencia (ex.: FastAPI/Python) for homologado e habilitado, `HttpPredictionProvider` realiza uma chamada `POST` JSON para `fireatlas.prediction.endpoint-url`.

### Cabecalhos HTTP enviados pelo Backend

- `Content-Type: application/json`
- `Accept: application/json`
- `X-Request-Id: <UUID da requisicao>`
- Sem redirecionamentos automaticos (`Redirect.NEVER`) e sem retentativas infinitas.

### Exemplo de Payload de Requisicao (`POST`)

```json
{
  "protocolVersion": "v1",
  "requestId": "550e8400-e29b-41d4-a716-446655440000",
  "bbox": {
    "minLon": -54.0,
    "minLat": -4.0,
    "maxLon": -53.0,
    "maxLat": -3.0
  },
  "asOf": "2026-11-13T12:00:00Z",
  "requestedWindow": {
    "start": "2026-11-14",
    "end": "2026-11-15"
  },
  "predictionTarget": "ACTIVE_FIRE_OCCURRENCE_PROBABILITY_DAILY_01DEG",
  "featureContractId": "firms-daily-grid-v1",
  "modelId": "fireatlas-xgb-pa",
  "modelVersion": "1.0.0"
}
```

### Exemplo Ilustrativo de Resposta do Servico de Inferencia (`200 OK`)

> **AVISO IMPORTANTE:** O JSON abaixo e **exclusivamente uma fixture ilustrativa de especificacao de protocolo** para orientar a implementacao futura do adaptador/servico. **Nao representa modelo existente nem resultado cientifico real.**

```json
{
  "protocolVersion": "v1",
  "executionId": "EXEC-20261113-0001",
  "modelId": "fireatlas-xgb-pa",
  "modelVersion": "1.0.0",
  "predictionTarget": "ACTIVE_FIRE_OCCURRENCE_PROBABILITY_DAILY_01DEG",
  "validatedHorizon": {
    "unit": "DAYS",
    "minLead": 1,
    "maxLead": 7,
    "temporalResolution": "P1D",
    "spatialResolution": "0.1_DEG"
  },
  "asOf": "2026-11-13T12:00:00Z",
  "predictedInterval": {
    "start": "2026-11-14",
    "end": "2026-11-15"
  },
  "coverage": {
    "status": "SUFFICIENT",
    "observedCoverageRatio": 0.94,
    "reason": null
  },
  "evaluationSummary": {
    "datasetId": "firms-pa-2020-2024-chronological",
    "temporalSplitPolicy": "WALK_FORWARD_CHRONOLOGICAL_2020_2024_TEST_2025",
    "metrics": {
      "prAuc": 0.61,
      "brierScore": 0.08
    }
  },
  "predictions": [
    {
      "lat": -3.45,
      "lon": -53.55,
      "forecastTimestamp": "2026-11-14T15:00:00Z",
      "occurrenceProbability": 0.72,
      "predictedFrpMw": 14.5,
      "frpUnit": "MW",
      "confidenceLevel": "NOMINAL"
    }
  ]
}
```

### Caso de Falta de Cobertura no Protocolo v1 (`coverage.status = "INSUFFICIENT_COVERAGE"`)

Se o servico remoto constatar que faltam observacoes ou covariaveis suficientes para o recorte (ex.: cobertura severa de nuvens ou lacuna de ingestao), deve responder HTTP `200` com:

- `coverage.status = "INSUFFICIENT_COVERAGE"`
- `coverage.reason` descrevendo a limitacao
- `predictions = null` (jamais enviar lista de zeros ou `predictions` preenchido junto com `INSUFFICIENT_COVERAGE`).

---

## 5. Configuracoes, Variaveis de Ambiente e Politica de Ativacao

A configuracao reside sob o prefixo `fireatlas.prediction` em `src/main/resources/application.yml`. **Por padrao, `fireatlas.prediction.enabled` e `false`.**

### Variaveis e Propriedades Suportadas

| Propriedade (`fireatlas.prediction.*`) | Padrao | Obrigatoria quando `enabled=true`? | Descricao |
|---|---|---|---|
| `enabled` (`PREDICTION_ENABLED`) | `false` | Sim (`true`) | Chave mestra operacional. Quando `false`, usa `DisabledPredictionProvider` mesmo se `endpoint-url` estiver definida. |
| `endpoint-url` (`PREDICTION_PROVIDER_URL`) | *(vazio)* | Sim | URL `http` ou `https` do servico de inferencia (configurada apenas no servidor, nunca pelo usuario). |
| `protocol-version` | `v1` | Sim (`v1`) | Versao do protocolo HTTP exigida pelo adaptador. |
| `connect-timeout-ms` (`PREDICTION_CONNECT_TIMEOUT_MS`) | `2000` | Sim (`1..60000`) | Timeout de conexao TCP em milissegundos. |
| `read-timeout-ms` (`PREDICTION_READ_TIMEOUT_MS`) | `5000` | Sim (`1..60000`) | Timeout de leitura da resposta HTTP em milissegundos. |
| `max-response-bytes` (`PREDICTION_MAX_RESPONSE_BYTES`) | `1048576` | Sim (`1024..10485760`) | Limite maximo de bytes lidos da resposta remota. |
| `max-predictions` (`PREDICTION_MAX_PREDICTIONS`) | `5000` | Sim (`1..50000`) | Quantidade maxima de pontos preditos aceitos por resposta. |
| `model-id` | *(nulo)* | Sim | Identificador unico do modelo aprovado. |
| `model-version` | *(nulo)* | Sim | Versao imutavel do modelo aprovado. |
| `prediction-target` | *(nulo)* | Sim | Identificador formal da variavel-alvo. |
| `feature-contract-id` | *(nulo)* | Sim | Identificador do contrato de features de entrada. |
| `supports-frp` | `false` | Sim | Indica se o modelo aprovado foi validado para prever `predictedFrpMw` em `MW`. |
| `horizon-unit` | *(nulo)* | Sim (`DAYS` ou `HOURS`) | Unidade do horizonte aprovado. |
| `min-lead` / `max-lead` | *(nulo)* | Sim (`0 <= min <= max`) | Faixa de antecedencia aprovada em relacao ao `asOf`. |
| `temporal-resolution` / `spatial-resolution` | *(nulo)* | Sim | Resolucao temporal (ex.: `P1D`) e espacial (ex.: `0.1_DEG`) homologadas. |
| `coverage-bbox` | *(nulo)* | Sim | Dominio geografico WGS84 (`minLon,minLat,maxLon,maxLat`) onde o modelo foi treinado e validado. |
| `artifact-uri` | *(nulo)* | Sim | Caminho/URI rastreavel do artefato do modelo. |
| `artifact-sha256` | *(nulo)* | Sim (64 hex) | Hash SHA-256 exato do artefato homologado. |
| `training-dataset-id` | *(nulo)* | Sim | Identificador do dataset historico documentado (`RN02`). |
| `evaluation-report-uri` | *(nulo)* | Sim | Referencia verificavel ao relatorio de avaliacao independente (`CA02`). |
| `temporal-split-policy` | *(nulo)* | Sim | Politica de separacao cronologica (rejeita `RANDOM`, `NONE`, `IN_SAMPLE`, `K_FOLD_RANDOM`) (`RN03`). |
| `baseline-comparison-summary` | *(nulo)* | Sim | Registro da comparacao contra baselines fora da amostra. |
| `out-of-sample-metrics.*` | *(vazio)* | Sim (mapa nao vazio) | Metricas finitas registradas no conjunto de teste fora da amostra (`CA02`). |
| `approved-by` / `approval-reference` / `approved-at` | *(nulo)* | Sim | Identificacao da equipe responsavel, referencia de aprovacao e timestamp UTC (`Instant`). |

### Bloqueio Cientifico Fail-Fast

- Um simples `enabled=true` ou `endpoint-url` isolado **nao habilita** o provedor: se qualquer campo de manifesto, avaliacao fora da amostra ou evidencia de aprovacao estiver ausente ou invalido, `PredictionModelProperties.validateForActivation()` lanca `IllegalStateException` e aborta a inicializacao do contexto Spring.
- Enquanto `enabled=false`, nenhuma dessas propriedades extras e exigida e a aplicacao inicializa normalmente com `DisabledPredictionProvider`.

---

## 6. Validacao de Input/Output, Cutoff Temporal (`asOf`) e Prevencao de Vazamento

1. **Propagacao explicita do cutoff (`asOf`):** a cada consulta, `PredictionService` captura o instante atual em UTC (`Clock`) e o envia no campo `asOf` da requisicao v1. O servico de inferencia **jamais** pode utilizar deteccoes FIRMS ou variaveis meteorologicas adquiridas apos `asOf` como features de entrada (`RN03`).
2. **Pre-validacao de escopo antes da chamada remota:** `HttpPredictionProvider` verifica se o `bbox` solicitado esta contido no `coverageBbox` aprovado e se `[start, end]` esta dentro de `[asOfDate + minLeadDays, asOfDate + maxLeadDays]`. Caso contrario, retorna imediatamente `OUT_OF_APPROVED_SCOPE` (`HTTP 200`, `available=false`, `predictions=null`) sem acionar o servico remoto.
3. **Validacao estrita da resposta remota (tudo-ou-nada):**
   - `protocolVersion == "v1"`, `executionId` nao vazio, `modelId`, `modelVersion`, `predictionTarget` e `validatedHorizon` identicos ao manifesto configurado;
   - `asOf` da resposta nao pode ser posterior ao `asOf` enviado; `predictedInterval` deve coincidir com `[start, end]`;
   - Cada item em `predictions` deve ter `lat` e `lon` finitos e contidos no `bbox` solicitado, `forecastTimestamp` nao anterior ao `asOf` e dentro da janela/horizonte contratado, `occurrenceProbability` finita em `[0.0, 1.0]` e, quando houver `predictedFrpMw`, `supportsFrp=true` no manifesto, valor finito `>= 0.0` e `frpUnit == "MW"`;
   - Se qualquer item for invalido, toda a resposta e descartada com `PredictionProviderException` (`INVALID_PROVIDER_RESPONSE`), garantindo ausencia total de resultados parciais.

---

## 7. Contrato HTTP Publico (`GET /api/v1/predictions`) e Estados de Indisponibilidade vs Falhas

### Cenarios e Respostas da API Publica

| Cenario | Status HTTP | Campo `available` | Campo `status` | Campo `predictions` | Descricao |
|---|---|---|---|---|---|
| **1. Modelo nao validado / desativado (padrao)** | `200 OK` | `false` | `MODEL_NOT_VALIDATED` | `null` | Nenhum modelo habilitado; todas as chaves de modelo/previsao presentes com `null`. |
| **2. Provedor habilitado, mas servico remoto indisponivel (conexao, timeout, HTTP 5xx)** | `503 Service Unavailable` | `false` | `PROVIDER_UNAVAILABLE` | *(ausente no ProblemDetail)* | `ProblemDetail` RFC 7807 sem expor URL interna, IP, segredo ou previsao parcial. |
| **3. Resposta remota invalida ou incompativel com contrato/manifesto** | `503 Service Unavailable` | `false` | `INVALID_PROVIDER_RESPONSE` | *(ausente no ProblemDetail)* | `ProblemDetail` RFC 7807; descarta integralmente respostas corrompidas ou fora de faixa. |
| **4. Janela ou regiao fora do escopo aprovado do modelo** | `200 OK` | `false` | `OUT_OF_APPROVED_SCOPE` | `null` | Recorte espacial ou janela temporal fora do dominio homologado no manifesto. |
| **5. Falta de cobertura observacional ou de variaveis no recorte** | `200 OK` | `false` | `INSUFFICIENT_DATA_COVERAGE` | `null` | `coverage.status = INSUFFICIENT_COVERAGE`; lacuna nunca e convertida em zero ocorrencias. |
| **6. Previsao valida (apenas quando houver modelo aprovado e resposta integra)** | `200 OK` | `true` | `AVAILABLE` | `[...]` | Lista validada de `PredictionPoint` + metadados do modelo e metricas fora da amostra. |
| **7. Parametros de consulta invalidos (`bbox`, `start`, `end`)** | `400 Bad Request` | *(N/A)* | `400` | *(ausente)* | `ProblemDetail` RFC 7807 (`NaN`, `Infinity`, `end < start`, `> 31` dias, etc.). |

### Resposta Padrao Atual (`200 OK`, `MODEL_NOT_VALIDATED`)

```json
{
  "available": false,
  "status": "MODEL_NOT_VALIDATED",
  "reason": "Nao existe modelo preditivo validado para o recorte solicitado. Os parametros start e end representam apenas a janela solicitada pelo cliente, nao um horizonte cientifico aprovado; anomalias historicas de /api/v1/timeseries nao constituem previsao.",
  "request": {
    "bbox": "-54.0,-4.0,-53.0,-3.0",
    "start": "2026-11-14",
    "end": "2026-11-15"
  },
  "predictionTarget": null,
  "validatedHorizon": null,
  "modelVersion": null,
  "evaluationMetrics": null,
  "predictions": null
}
```

---

## 8. Guia para Conectar um Futuro Servico Python (Sem Alterar o Spring Boot)

Quando a equipe cientifica concluir e aprovar um modelo (por exemplo, em Python/FastAPI):

1. **Implementar o endpoint `POST /v1/predict`** recebendo o JSON `V1PredictRequest` e respondendo `V1PredictResponse` conforme a Secao 4 deste documento.
2. **Respeitar o `asOf`:** filtrar todas as consultas de features para `timestamp <= asOf` (em UTC).
3. **Tratar falta de cobertura explicitamente:** se a regiao/janela nao tiver observacoes ou meteorologia suficiente, retornar `coverage.status = "INSUFFICIENT_COVERAGE"` e `predictions = null` (nunca retornar lista vazia `[]` com `SUFFICIENT`, nem probabilidades sinteticas `0.0`).
4. **Preencher o manifesto em variaveis de ambiente ou `application.yml`** (`fireatlas.prediction.*`, incluindo `artifact-sha256`, `evaluation-report-uri`, `temporal-split-policy`, `out-of-sample-metrics` e `approved-by`) e somente entao definir `PREDICTION_ENABLED=true` e `PREDICTION_PROVIDER_URL=http://<host-interno>/v1/predict`.

---

## 9. Fontes de Dados Existentes, Variaveis Adicionais, Separacao Temporal, Baselines e Metricas

- **Fontes atuais e limitacoes:** tabelas `detection`, `daily_grid`, `ingestion_log` e fixture 24h `src/test/resources/fixtures/firms_brazil_2026_11_12_13/` (`31` MODIS, `82` VIIRS `N20`, `83` VIIRS `N21`). Serie VIIRS S-NPP encerrada em **2026-11-02** pela NASA FIRMS; diferenca fisica entre MODIS (`1 km`) e VIIRS (`375 m`); latencia provisoria `URT`/`RT`/`NRT` vs `Standard`; ausencia de deteccao != ausencia de fogo (`unknown/no-data`); produtos FIRMS nao se destinam a preservacao de vida ou propriedade.
- **Variaveis adicionais necessarias para futuro modelo:** mascara de observabilidade/nuvens (`no-data` vs zero observado), meteorologia historica/prevista sem vazamento (temperatura, umidade, VPD, vento, precipitacao) e bioma/combustivel.
- **Separacao temporal obrigatoria (`RN03`):** divisao estritamente cronologica (`treino < validacao < teste`, walk-forward), proibindo split aleatorio (`RANDOM` / `K_FOLD_RANDOM` bloqueados em `PredictionModelProperties`).
- **Baselines minimos para comparacao futura:** persistencia temporal e climatologia sazonal sobre a mesma mascara de observabilidade.
- **Metricas candidatas por tipo de alvo:** PR-AUC, Brier Score e curva de calibracao para probabilidades binarias; MAE, RMSE e pinball loss para FRP condicional.

---

## 10. Matriz `RN01`–`RN05` / `CA01`–`CA03` e Distincao Conceitual

| Regra / Criterio | Estado atual | Evidencia |
|---|---|---|
| `RN01`–`RN04` | **Protegidos por bloqueio (`PredictionModelProperties`)** | Ativacao exige manifesto completo de alvo, horizonte, dataset, split cronologico e avaliacao fora da amostra; desativado por padrao. |
| `RN05` | **Implementado e verificado** | Padrao `DisabledPredictionProvider` retorna `MODEL_NOT_VALIDATED` com `available=false` e campos `null`. |
| `CA01` / `CA02` | **Infraestrutura pronta; modelo cientifico pendente** | Porta `PredictionProvider` e `HttpPredictionProvider` validados com testes de contrato; nenhum modelo real habilitado em producao. |
| `CA03` | **Implementado e verificado** | API informa `MODEL_NOT_VALIDATED` (ou `OUT_OF_APPROVED_SCOPE` / `INSUFFICIENT_DATA_COVERAGE`) em HTTP `200` e `503` em falhas remotas. |

| Conceito | Endpoint | Natureza | E previsao futura? |
|---|---|---|---|
| **Deteccoes observadas** | `/detections`, `/calendar`, `/compare`, `/harmonized` | Focos adquiridos no passado por MODIS/VIIRS (`harmonized` = `METHOD_NOT_APPROVED`). | **Nao** |
| **Anomalias historicas** | `/timeseries` (`Anomaly.robustZ`) | Z-score robusto (mediana/MAD, `3.5`) retrospectivo sobre o proprio intervalo consultado. | **Nao** |
| **Previsoes futuras** | `/predictions` | Porta hexagonal + adaptador HTTP v1; hoje em `MODEL_NOT_VALIDATED` (`available=false`). | Indisponivel sem modelo aprovado |

---

## 11. Pendencias pos-integracao do modelo preditivo — NASA-020

- [ ] Alvo (`predictionTarget`), sensor/grade e horizonte (`validatedHorizon`, ex.: `D+1` a `D+7` ou `HOURS`) formalmente aprovados.
- [ ] Dataset multianual documentado (`trainingDatasetId`) em alinhamento com o repositorio de dados e experimentos (`https://github.com/CaiqueSBrito/fire-atlas-data`), com mascara `unknown/no-data` e sem vazamento temporal (`temporalSplitPolicy` cronologica).
- [ ] Revisao de compatibilidade entre o horizonte `D+1`–`D+7`, indexacao espacial (ex.: celulas `H3` vs grade `0,1°` / coordenadas centroide `lat`/`lon` em WGS84) e interfaces da API (`GET /api/v1/predictions` e protocolo interno HTTP `v1`).
- [ ] Modelo avaliado fora da amostra superando persistencia e climatologia (`evaluationReportUri` e `outOfSampleMetrics`), mantendo `CA01` e `CA02` da NASA-020 pendentes ate essa homologacao.
- [ ] Artefato congelado com hash SHA-256 (`artifactUri` e `artifactSha256`) e aprovacao formal (`approvedBy`, `approvalReference`, `approvedAt`).
- [ ] Testes ponta a ponta e validacao cientifica antes de habilitar previsoes (`PREDICTION_ENABLED=true`).
- [ ] Reavaliacao posterior da NASA-018 (interoperabilidade com o pipeline de dados, linhagem e proveniencia), mantendo a distincao estrita entre previsao experimental e harmonizacao cientificamente aprovada (`CA02` da NASA-018 permanece `METHOD_NOT_APPROVED`).
- [ ] Acompanhamento centralizado na Issue #2 (`https://github.com/CaiqueSBrito/fire-atlas-api/issues/2`), que permanece ABERTA ate a integracao e homologacao cientifica do modelo.


---

## 12. Pendências pós-integração do modelo preditivo — NASA-020

### A. Infraestrutura técnica validada agora

O backend dispõe de `PredictionController`, `PredictionService`, porta `PredictionProvider`, provedor desativado por padrão e adaptador HTTP de contrato versionado `v1`. Os testes verificam indisponibilidade sem modelo, ausência de chamadas externas no estado padrão, validação de configuração e respostas simuladas de contrato. Essas fixtures não constituem resultados científicos reais nem aprovação de um modelo.

A homologação técnica final e a publicação da NASA-020 ainda dependem das correções do Gate C.1: garantir timeout durante toda a leitura do corpo HTTP, respeitar a antecedência mínima aprovada em horas e alinhar a variável de endpoint documentada à configuração efetiva. A aprovação dos testes existentes, isoladamente, não resolve esses achados. Nenhum commit de entrega da NASA-020 foi publicado neste gate.

### B. Integração do modelo pendente

O modelo preditivo ainda não foi fornecido. Após sua disponibilização, executar e registrar:

- [ ] Integrar o serviço de inferência real, identificando artefato, versão e checksum imutável.
- [ ] Conferir a compatibilidade entre o modelo e o contrato HTTP versionado, incluindo tipos, unidades, estados de indisponibilidade e metadados.
- [ ] Mapear as features efetivamente disponíveis, suas fontes, cobertura, latência e regras para dados ausentes.
- [ ] Verificar cobertura geográfica e temporal, latência e comportamento sob falhas, respostas inválidas e interrupções de leitura.
- [ ] Executar testes ponta a ponta utilizando o modelo real, com referências reproduzíveis e resultados esperados documentados.
- [ ] Reavaliar segurança e desempenho do serviço e do adaptador no ambiente integrado.
- [ ] Registrar e corrigir os problemas técnicos identificados somente após a integração, antes de habilitar o serviço.

### C. Validação científica pendente e condições de habilitação

- [ ] Definir e obter aprovação formal do alvo, horizonte, resoluções e domínio geográfico/temporal do modelo.
- [ ] Documentar separação temporal entre treino, validação e teste, disponibilidade das features no instante `asOf` e prevenção de vazamento.
- [ ] Realizar avaliação independente fora da amostra, com métricas documentadas, baselines, incertezas e limitações.
- [ ] Reavaliar individualmente RN01–RN05 e CA01–CA03 com evidências da integração real.
- [ ] Para habilitar previsões em produção, concluir as correções técnicas, os testes de contrato e ponta a ponta, a avaliação de segurança/desempenho e a aprovação científica; preencher integralmente o manifesto, incluindo referência de aprovação, e somente então habilitar o provedor.

**CA01 e CA02 permanecem condicionais e não concluídos.** CA03 deve preservar a indisponibilidade explícita quando faltarem modelo, método, cobertura ou condições exigidas. Até a homologação, manter `enabled=false`, `MODEL_NOT_VALIDATED`, `available=false` e previsões nulas. A referência inicial `336031a4c1bcf07a5ebb6cb6f10e4782c919cbbc` pertence à baseline anterior; não é commit de entrega da NASA-020. Registrar o SHA de entrega somente após a aprovação e publicação futuras.

Acompanhamento: [NASA-020 | Integrar e homologar modelo preditivo quando disponível](https://github.com/CaiqueSBrito/fire-atlas-api/issues/2).
