# Harmonizacao MODIS × VIIRS — Contrato e Estado Cientifico (NASA-018)

> **Estado atual:** `METHOD_NOT_APPROVED` (correspondencia exploratoria `exploratory-spatial-temporal-v0.2`; **nao possui aprovacao cientifica**).

## 1. Diferencas Fisicas e Operacionais entre Sensores

MODIS e VIIRS possuem caracteristicas instrumentais e orbitais fundamentalmente distintas:

- **Resolucao espacial e geometria de varredura:** o MODIS possui pixel nominal de ~1 km no nadir (crescendo ate ~2 × 4,8 km nas bordas da faixa conforme `scan`/`track`), enquanto o VIIRS possui pixel nominal de ~375 m no nadir (crescendo mais moderadamente fora do nadir). Centros proximos nao garantem que dois registros pertencam ao mesmo pixel fisico nem ao mesmo incendio.
- **Sensibilidade de deteccao:** o VIIRS detecta focos menores e mais frios do que o MODIS; contagens brutas e somas de FRP nao sao diretamente comparaveis nem equivalentes.
- **Horarios de passagem orbital (UTC e deriva orbital):** satelites de orbita heliossincrona cruzam o equador em horarios solares locais proximos (historicamente ~10:30/22:30 local no Terra e ~13:30/01:30 local no Aqua, NOAA-20 e NOAA-21, sujeitos a deriva orbital gradual como ocorreu no Aqua). Como a conversao para UTC depende da longitude da observacao, nao ha horario UTC universal fixo no globo: em observacoes no Brasil (~45°W–60°W), exemplos empiricos recentes situam passagens em torno de ~01/11 UTC (Terra), ~06/18 UTC (Aqua apos deriva) e ~04–05/16–17 UTC (NOAA-20/NOAA-21). Por essa defasagem orbital, associacoes dentro da janela exploratoria refletem proximidade temporal, nao observacao simultanea.
- **Continuidade de plataformas (atualizacao operacional FIRMS):** a NASA FIRMS encerrou a serie VIIRS S-NPP em **2026-11-02**. Dados recentes apos essa data dependem de VIIRS NOAA-20 e NOAA-21, sem calibracao automatica entre plataformas ou entre MODIS e VIIRS.
- **Fluxo de processamento (`URT`, `RT`, `NRT` vs `Standard`):** versoes terminadas em `URT` (Ultra Real-Time, ex.: `6.1URT`, `2.0URT`), `RT` (Real-Time, ex.: `6.1RT`, `2.0RT`) e `NRT` (Near Real-Time, ex.: `6.1NRT`, `2.0NRT`) pertencem a fluxos operacionais provisórios de baixa latencia sujeitos a revisao posterior, enquanto tokens explicitamente documentados sem sufixo de tempo real (`6.0`, `6.1`, `1`, `1.0`, `2`, `2.0`) representam arquivo cientifico definitivo (`Standard`). Valores desconhecidos ou nao documentados nao devem ser presumidos como `Standard` por exclusao. Ambos preservam seu identificador bruto em `version`.
- **Confianca nativa e validacao na ingestao:** MODIS reporta inteiro numerico (`0..100` em `confidence_raw`), enquanto VIIRS reporta categorias (`low`/`nominal`/`high` ou `l`/`n`/`h` em `confidence_raw`). Na ingestao (`DetectionRowParser`), campos vazios permanecem `NULL` sem invencao, enquanto valores nao negativos sao exigidos para FRP (`>= 0`), horario UTC estrito `0000..2359` e tokens fora da escala documentada de cada sensor (ex.: MODIS `> 100` ou texto desconhecido) sao rejeitados como linhas malformadas (nunca convertidos automaticamente em `'h'` ou `'l'`). O campo `confidence_class` (`l`/`n`/`h`) e apenas agrupamento auxiliar, **nao** equivalencia cientifica.
- **Ausencia de dados:** ausencia de deteccoes ou de pares candidatos nao comprova ausencia de fogo nem cobertura orbital continua (nuvens, fumaca, angulo de visada ou recorte de ingestao).
- **Ressalva de uso:** produtos FIRMS nao se destinam a preservacao de vida ou propriedade; nenhuma classificacao de incendio confirmado ou falso positivo e avaliada neste backend.

---

## 2. Baseline Congelado de `GET /api/v1/harmonized` (Etapas B0, B.1 e B.2)

### Parametros publicos
- `bbox` (obrigatorio): `minLon,minLat,maxLon,maxLat` (coordenadas finitas WGS84, `-180..180`, `-90..90`, `minLon < maxLon`, `minLat < maxLat`).
- `start`, `end` (obrigatorios): datas UTC `YYYY-MM-DD` (`end >= start`, janela maxima de 31 dias).
- `maxDistanceMeters` (opcional, padrao `750`, faixa `100..5000`): limiar geodesico WGS84 inclusivo (`<=`), **experimental/nao aprovado cientificamente**.
- `maxMinutes` (opcional, padrao `180`, faixa `1..720`): limiar temporal UTC absoluto inclusivo (`<=`), **experimental/nao aprovado cientificamente**.
- `modisSampleCap` (opcional, padrao `20000`, faixa `100..100000`): teto deterministico (`ORDER BY acq_ts ASC, acq_date ASC, id ASC`) de registros MODIS avaliados.
- `includeAssociations` (opcional, **padrao `false`**, opt-in): omite por padrao a lista `associations` para reduzir payload HTTP; envie `?includeAssociations=true&associationsLimit=...` para receber os pares individuais. **Observacao operacional:** o calculo do resumo (`summary`) continua avaliando os pares candidatos no SQL, portanto `includeAssociations=false` economiza materializacao de `top_p`, transferencia JDBC e serializacao JSON, nao eliminando o custo integral da juncao espaco-temporal no banco.
- `associationsLimit` (opcional, padrao `500`, faixa `1..5000`): teto deterministico de pares retornados quando `includeAssociations=true`.

### Compatibilidade, Identidade, Proveniencia e Controle Transacional
- Campos de topo preservados: `bbox`, `start`, `end`, `params`, `result`, `method`, `message`.
- Correcoes de integridade implementadas:
  - **Identidade local vs. rastreabilidade da fonte (`V2` + `V3` + `V4` + `V5` + `V6` + DTO):**
    - *Identidade da observacao na base:* chave primaria composta particionada `(acq_date, id)`, exposta em `DetectionObservation` via `id` (legado), `acquiredDate` e `observationKey` (`"<acq_date>:<id>"`).
    - *Localizador da linha bruta:* `sourceFile`, `sourceLineNumber`, `sourceRawLocator` (`"<sourceFile>#L<line>"`) e `sourceRowHash` (SHA-256 integral da linha textual CSV recebida).
    - *Identidade/assinatura da interpretacao cientifica e versionamento (`V6`):* `sourceSemanticHash` (SHA-256 canonico sob versao atual `v2-ieee754-direct`, conversao direta decimal -> IEEE-754 binary32/`real` e binary64/`double precision` sem duplo arredondamento, distinguindo `NULL` de `0.0`), acompanhado em `DetectionObservation` por `sourceSemanticHashStatus`, `sourceSemanticHashVersion` e `legacySourceSemanticHash`:
      - `CURRENT` (`v2-ieee754-direct`): assinatura verificada pelo algoritmo canonico atual.
      - `UNKNOWN` (`null`): registro pre-V3/V4 ou com proveniencia insuficiente; `sourceSemanticHash` permanece `null` sem invencao.
      - `LEGACY_UNVERIFIED` (`v1-legacy-unverified`): registros pre-V6 com assinatura historica nao versionada (ex.: parser float32 anterior a correcao da V5/B.3) sao isolados em `legacySourceSemanticHash` durante a migration `V6` com `sourceSemanticHash = NULL` (sem conversao automatica em massa e sem expor hash nao verificado como atual, evitando inclusive colisoes artificiais no indice `uq_detection_source_line_semantic`).
      - `UPDATED_FROM_LEGACY` (`v2-ieee754-direct`): quando uma retransmissao integralmente comprovada (coincidindo `(acq_date, source_file, source_line_number)`, `source_row_hash`, `source_file_sha256` do CSV completo com cabecalho e todos os valores cientificos persistidos) confirma o registro historico, o `id` original e os valores cientificos permanecem intactos, `sourceSemanticHash` e promovido para a assinatura atual e `legacySourceSemanticHash` e preservado para auditoria. Qualquer divergencia no checksum do arquivo ou semantica impede falsa estabilidade e sinaliza ambiguidades.
    - *Revisao do arquivo:* `sourceFileSha256` (SHA-256 integral do CSV quando lido ate EOF).
    - *Chave compacta `sourceRecordKey`:* formato `"<sourceFile>#L<line>:raw=<raw12>:sem=<sem12>"` (ou `:legacySem=<legacy12>` quando `LEGACY_UNVERIFIED`; omitindo componente ausente em registros parciais; `null` em registros pre-V3 sem proveniencia). Prefixos abreviados de 12 caracteres em `sourceRecordKey` servem apenas para inspecao legivel e **nao** garantem unicidade criptografica universal; comparacoes exatas devem inspecionar os hashes integrais (`sourceRowHash`, `sourceSemanticHash`, `legacySourceSemanticHash`, `sourceFileSha256`) e `observationKey`.
  - **Unicidade semantica e revisoes parciais (`V5` + `V6`):**
    - A restricao `uq_detection_source_line_semantic` garante unicidade por `(acq_date, source_file, source_line_number, source_semantic_hash)` apenas quando `source_semantic_hash IS NOT NULL` (certificado), enquanto `idx_detection_source_raw_hash` e `idx_detection_semantic_status` apoiam reconciliacao historica e auditoria.
    - Alteracoes de cabecalho com mesmo texto de linha divergem em interpretacao (`source_semantic_hash`) e nunca usam apenas `source_row_hash` para presumir igualdade semantica: fora da bbox o registro antigo permanece intacto (inclusive quando `LEGACY_UNVERIFIED`, sem conflito artificial em V5); dentro da bbox o registro obsoleto e substituido no `Passo A`/`Passo B` sem mascarar conflitos via `ON CONFLICT DO NOTHING`.
  - **Atomicidade transacional e protecao contra `commit()` apos excecao (`Savepoint` JDBC):** `ingestOnConnection` exige transacao explicita (`autoCommit=false`) de exatamente uma origem (`fireatlas.ingest_tx_source` definido antes do savepoint). O `Savepoint` garante rollback integral em caso de falha mesmo se o chamador capturar a excecao e executar `commit()`, enquanto `pg_advisory_xact_lock` adquiridos apos o savepoint sao liberados pelo PostgreSQL durante `ROLLBACK TO SAVEPOINT`; manter `fireatlas.ingest_tx_source` ativo impede reutilizacao inconsistente da transacao externa apos falha.
  - **Revisoes de arquivo, mudanca de posicao e registros legados:** cada `source_line_number` preserva registros legitimamente duplicados em linhas distintas do mesmo CSV. Registros legados pre-`V3`/`V4` mantem `NULL` nas colunas de proveniencia e campos equivalentes de `DetectionObservation` sem fabricacao retroativa, sendo sinalizados em `reasons` quando consultados (`CA03`).
  - **Leitura integral fail-safe e namespace de staging (`pg_temp`):** no fluxo padrao que reconcilia dados persistentes, `DetectionCsvLoader` le o arquivo CSV integralmente ate o EOF sem supor ordenacao cronologica nem parar apos 600.000 linhas. As tabelas de staging e recarga (`pg_temp.tmp_detection_stage`, `pg_temp.tmp_affected_grid`) sao qualificadas estritamente em `pg_temp`, preservando intactas eventuais tabelas permanentes homonimas em `public`.
  - **Locks em fluxo concorrente (`IngestionRunner`):** Fase 1 (`pg_advisory_xact_lock` por `sourceKey`, espaco nao negativo) -> Fase 2 (`pg_advisory_xact_lock` ordenado por celula `daily_grid`, espaco negativo). Lotes multi-origem na mesma transacao nao sao suportados.
  - **Consistencia incremental da grade (`daily_grid`):** toda alteracao transacional confirmada em `detection` (incluindo recargas vazias integralmente lidas ou remocoes que zeram uma data/celula) recalcula exclusivamente as celulas `(acq_date, sensor, cx, cy)` afetadas sob bloqueio consultivo ordenado.
  - **FRP e metadados nativos:** MODIS com `frp IS NULL` nao e mais excluido da avaliacao; `frp` ausente permanece `null` (nunca convertido em `0`), mantendo valor original em MW e unidade (`frpUnit="MW"`).
  - **Relacoes N:M sem dupla contagem:** todas as relacoes candidatas `N:M` sao preservadas sem atribuicao exclusiva 1:1 e sem dupla contagem de VIIRS distintos ou de FRP.
  - **Distancia geodesica exata e pre-filtro GiST (`geography`):** `idx_detection_geog` indexa caixas envolventes cartesianas geocentricas 3D `(X, Y, Z)` sobre `geom::geography`. Como `ST_DWithin(geog, geog, dist)` pode divergir na borda numerica exata quando `ST_Distance <= dist` e verdadeiro, a consulta aplica pre-filtro de indice `ST_DWithin(..., :dist + 1.0)` apenas para candidatos e decide o limiar cientifico estritamente por `ST_Distance(v.geom::geography, m.geom::geography) <= :dist`, cobrindo antimeridiano e altas latitudes sem ampliar o limiar configurado.
  - **Ordenacao deterministica:** amostragem (`acq_ts, acq_date, id`) e pares (`m_acq_ts, m_acq_date, m_id, distance_meters, abs(time_offset_seconds), v_acq_ts, v_acq_date, v_id`), com flags exatas `sampled` e `associationsTruncated`.
  - **Orcamento de tempo SQL na transacao:** `HarmonizationService` compartilha `HARMONIZE_TIMEOUT_MS` (padrao `10000` ms) entre os statements SQL da transacao ajustando `SET LOCAL statement_timeout` pelo saldo restante e abortando antes do segundo statement caso o orcamento expire (HTTP `504` RFC 7807 `ProblemDetail`, sem metricas parciais). Esse controle limita o tempo de execucao SQL no PostgreSQL, nao garantindo tempo maximo de socket/rede HTTP ou serializacao no servidor web.

### Indicadores legados explicitamente indisponiveis
Os campos legados de `result` abaixo permanecem presentes com valor `null` e motivo documentado em `unavailableIndicators`:
- `meanModisFrp` -> `METHOD_NOT_APPROVED`
- `meanViirsFrpSum` -> `METHOD_NOT_APPROVED` (dupla contagem N:M, ausencia de regra fisica para soma e risco de coercao de `frp` ausente)
- `viirsToModisFrpRatio` -> `METHOD_NOT_APPROVED` (razao nao constitui fator de calibracao/conversao validado)
- `frpCorrelation` -> `METHOD_NOT_APPROVED` (correlacao sem calibracao nem sincronizacao orbital)
- `medianViirsPixelsPerModis` -> `METHOD_NOT_APPROVED` (centroide/raio fixo nao equivale a pegada real do pixel MODIS nem atribuicao exclusiva)
- `matchRate` -> `NOT_COMPUTABLE` quando um ou ambos os sensores estao ausentes no recorte (calculavel como taxa descritiva exploratoria quando ha observacoes MODIS e VIIRS).

### Estados cientificos e operacionais (`scientificState`)
- `correspondenceMode`: `EXPLORATORY_ASSOCIATION` (quando ha base para avaliar correspondencia exploratoria) ou `NOT_COMPUTABLE` (ausencia de MODIS, ausencia de VIIRS ou ausencia de deteccoes).
- `harmonizationStatus`: `METHOD_NOT_APPROVED` (sem metodo cientificamente validado neste repositorio).
- `derivedClassificationStatus`: `NOT_EVALUATED` (nao classifica incendio confirmado nem falso positivo).
- `confidenceSemantics`: `PRODUCT_NATIVE_NOT_EQUIVALENT`.
- `dataAvailabilityStatus`: `BOTH_SENSORS_WITH_CANDIDATE_PAIRS`, `BOTH_SENSORS_NO_CANDIDATE_PAIRS`, `MODIS_ONLY`, `VIIRS_ONLY`, `NO_DETECTIONS`, ou `MODIS_IN_WINDOW_WITH_BOUNDARY_VIIRS_CANDIDATES`.

---

## 3. Amostras Reais Verificaveis e Limites de Validacao (Etapa B6)

O conjunto `src/test/resources/fixtures/firms_brazil_2026_11_12_13/` contem um recorte oficial NASA FIRMS 24h (`2026-11-12` a `2026-11-13`, regiao Para/Brasil `-54,-4,-53,-3`) com manifestos SHA-256 (`MANIFEST.json`), cobrindo:
- `MODIS_C6.1` (`MODIS_C6_1_South_America_24h.csv`, 31 deteccoes Aqua/Terra)
- `VIIRS_NOAA-20` (`J1_VIIRS_C2_South_America_24h.csv`, 82 deteccoes `N20`)
- `VIIRS_NOAA-21` (`J2_VIIRS_C2_South_America_24h.csv`, 83 deteccoes `N21`)

Valores esperados foram calculados de forma independente via formula geodesica de Vincenty (WGS84, convergencia `1e-12` rad, tolerancia de comparacao vs PostGIS `<= 0,50 m`) e aritmetica UTC pelo oraculo versionado `src/test/java/com/fireatlas/oracle/FirmsIndependentOracle.java` (31 MODIS, 165 VIIRS, 12 MODIS com candidatos, 44 VIIRS distintos compartilhados em 58 pares N:M dentro de 750 m / 180 min).

Reproducao independente sem Spring/PostGIS (JDK 21+):

```bash
java src/test/java/com/fireatlas/oracle/FirmsIndependentOracle.java
```

**Criterios de Aceite da NASA-018 (`CA01`–`CA04`):**
- **`CA01` (Correspondencia):** apresentar associacoes candidatas identificadas e seus registros completos de origem (`observationKey`, `sourceLineNumber`, `sourceRawLocator`, `sourceRowHash`, `sourceSemanticHash`, `sourceFileSha256` quando disponiveis) conforme criterios espaciais/temporais explicitos.
- **`CA02` (Harmonizacao):** permanece **`METHOD_NOT_APPROVED`** (bloqueado) enquanto nao houver metodologia cientifica validada pela equipe; valores originais permanecem intactos e indicadores de equivalencia continuam nulos.
- **`CA03` (Dados insuficientes e proveniencia incompleta):** quando faltam sensores (`NO_DETECTIONS`, `MODIS_ONLY`, `VIIRS_ONLY`), FRP (`null` preservada sem zero sintetico) ou proveniencia completa (inclusive campos `V3`/`V4`: `source_line_number`, `source_row_hash`, `source_file_sha256`, `source_semantic_hash`), a API sinaliza explicitamente em `scientificState`, `reasons` e `unavailableIndicators` sem inventar valores. Leituras interrompidas ou falhas capturadas nao gravam alteracoes parciais nem sucesso em `ingestion_log`.
- **`CA04` (Validacao com amostra de referencia):** comparacao com a amostra de referencia FIRMS em `src/test/resources/fixtures/firms_brazil_2026_11_12_13/`, cujos 58 pares gerados pelo endpoint sao reproduziveis e compativeis com os valores esperados geometricos e temporais documentados pelo oraculo independente. **Essa conferencia valida exclusivamente a implementacao exploratoria geometrica/temporal e a rastreabilidade dos dados — nao comprova equivalencia fisica nem aprova metodologia cientifica**, e qualquer classificacao derivada (incendio confirmado ou falso positivo) permanece **`NOT_EVALUATED`** (distinta da confianca nativa de cada produto).
