# Contrato da API (v1)

Base: `/api/v1`. JSON. Sem login. Documentação interativa: `/swagger-ui.html` (OpenAPI em `/v3/api-docs`).

**Fonte dos dados:** camada ouro do data lake (`gold/map_grid`, Delta Lake): uma linha por **célula H3, dia e sensor**, já
agregada. Não há focos individuais. A API **só consome H3**: não converte `bbox` em células nem célula em coordenadas.

## Convenções

| Item | Regra |
|---|---|
| **Região** | Exatamente um entre: `cells=<índices H3 em hexadecimal, separados por vírgula>` (até 2000 células, resoluções de 0 a 6), `country=<ISO de 2 ou 3 letras>`, `continent=<nome>` ou `city=<geonameId>` (com `cityRadiusKm`). **`bbox` não é aceito** (400) |
| **Período** | `start` e `end` no formato `YYYY-MM-DD` (inclusivos) |
| **Sensor** | `sensor=MODIS`, `VIIRS` ou `BOTH` (padrão) |
| **Resolução usada** | A das próprias células informadas (a gold já traz cada resolução agregada). Se a região for grande demais para a consulta, a resolução é reduzida e a resposta informa `resolution` |
| **Sem dados** | Resposta `200` vazia com `message` ("Nenhum foco de calor encontrado..."), sem números inventados |
| **Cache** | `Cache-Control: public, max-age=60` (dados recentes) ou `600` (históricos) |

### Camadas distinguíveis (RN02)
As células agregadas, o calendário e as estatísticas separam MODIS e VIIRS. A **harmonização e as correspondências** MODIS × VIIRS dependem de focos individuais, que a gold agregada não tem: elas ficam no app principal (`/api/v1/harmonized`), que lê os focos da camada prata.

### Identificação de indicadores (RN04)
Respostas com estatísticas trazem `meta`: `region` e período (**recorte**), `source` (**fonte**: gold/map_grid), `units`
(**unidades**: detecções, FRP em MW, resolução em m) e `method` (**método de cálculo**).

### Erros (RN06, CA04)
`400` no formato RFC 7807, com **cada parâmetro rejeitado**:
```json
{
  "title": "Parametros invalidos",
  "status": 400,
  "detail": "bbox: esta consulta agregada so aceita cells (indices H3): a API nao converte bbox em celulas; endA deve ser igual ou posterior a startA",
  "errors": [
    {"parameter": "bbox", "message": "esta consulta agregada so aceita cells (indices H3): a API nao converte bbox em celulas"},
    {"parameter": "endA", "message": "endA deve ser igual ou posterior a startA"}
  ]
}
```
`503` = banco indisponível (mensagem genérica, sem dados de conexão);
`500` = erro interno genérico.

## Filtro por lugar (país, continente e cidade)

Qualquer consulta que aceita `cells` aceita, no lugar dele, **um** destes (nunca dois):

| Parâmetro | Valor | Exemplo |
|---|---|---|
| `country` | código ISO do país, de 2 ou 3 letras | `country=BR` ou `country=BRA` |
| `continent` | nome do continente, em inglês ou português, sem diferenciar acento | `continent=Europe`, `continent=América do Sul` |
| `city` | `geonameId` da cidade (veja `/places/cities`) | `city=3451190&cityRadiusKm=25` |

Funciona em `/detections`, `/detections/count`, `/calendar`, `/compare`, `/compare/periods`, `/compare/historical`, `/timeseries`, `/rankings/cells` e `/forecast`. Os valores válidos saem de `/places/*`.

**Como o lugar é aplicado (limites):** a gold (`h3_places`, do time de dados) atribui cada célula H3 a **um país**, o predominante nela, e à **cidade mais próxima** do centro dela (GeoNames `cities1000`).
- **País e continente:** nas fronteiras o filtro é aproximado, na ordem do tamanho da célula. O mapa usa a resolução 4 por padrão (ou a do `zoom`/`resolution`).
- **Cidade:** não é "dentro da cidade". É o conjunto de células cuja cidade mais próxima é a pedida, **a até `cityRadiusKm`** (padrão 25 km, de 1 a 500). A cidade só existe nas resoluções 5 e 6, então a consulta usa a resolução 5 ou mais; **os dados dessa resolução precisam estar importados**.
- Célula sem país (oceano) não entra em nenhum filtro por lugar.
- A resposta traz `meta.region` (ex.: `pais:BR`, `continente:Europe`, `cidade:3451190 (ate 25.0 km)`).

## Endpoints

### `GET /places/continents`, `/places/countries`, `/places/cities` — catálogo de lugares
- `/places/continents`: `{name, countries}`.
- `/places/countries?continent=`: `{isoA2, isoA3, name, namePt, continent, cells}`.
- `/places/cities?country=BR&q=sao&cityRadiusKm=25&limit=50`: `{geonameId, name, countryCode, cells, nearestKm}` (as com mais células primeiro). `cells` = quantas células de resolução 5 têm essa cidade como a mais próxima dentro do raio.
Todas trazem `source` e `note` (como o lugar é atribuído às células).

### `GET /forecast` — previsão de fogo por célula
Parâmetros: região (`cells`, `country`, `continent` ou `city`), `issueDate` (YYYY-MM-DD; padrão: a mais recente), `minProbability` (0 a 1, padrão 0), `onlyActive` (padrão false), `limit` (1–5000, padrão 500).
Devolve as células de **resolução 6** da região, da maior para a menor probabilidade: `{h3, region, probability7d, detections24h, detections3d, detections7d, frpMean7d, modisBrightnessMean7d, viirsBrightnessMean7d, ring1Detections24h, ring1Detections7d, ring2Detections7d, active}`, mais `issueDate`, `latestIssueDate`, `truncated`, `source` e `method`.
- `probability7d` é a probabilidade (0 a 1) de fogo na célula nos próximos 7 dias, **calculada pelo pipeline de dados** (modelo do time de dados). A API não recalcula nem ajusta.
- Campos nulos na fonte ficam ausentes (nada é inventado). Sem previsão para o pedido: `count=0` e `message` (ex.: indica a emissão mais recente).
- `GET /forecast/issues` lista as datas de emissão disponíveis (`issueDate`, `cells`, `activeCells`).
- Quem gera e atualiza a tabela é o time de dados; este serviço só importa (`--ingest.dataset=forecast`).

### `GET /detections` — células H3 agregadas
Parâmetros: `cells`, `start`, `end`, `sensor`, `zoom` (0–22, padrão 6). Devolve `mode: "grid"` e células
`{sensor, h3, resolution, count, frpSum, frpMax}` (sem coordenadas) na resolução do zoom (3 a 6). Não há pontos individuais.

### `GET /detections/count` — contagem por sensor
Parâmetros: `cells`, `start`, `end`, `sensor`. Devolve `total` e, por sensor, `detections`, `frpSum` (MW) e `daysWithData`,
mais `meta`. Sem registros: `total=0`, `sensors` vazio e `message`.

### `GET /calendar` — cartas por dia
Parâmetros: `cells`, `month=YYYY-MM`, `sensor`. Um item por dia do mês: `{date, modis:{count,frpSum}, viirs:{count,frpSum}}`,
`daysWithFire` e `resolution`.

### `GET /compare` — comparação entre sensores
Parâmetros: `cells`, `start`, `end`, `sensor`. Por sensor: `detections`, `daysWithData`, `frpMean` (soma do FRP ÷ detecções),
`frpMax`, `frpTotal` (MW), `nominalResolutionMeters` e a confiança como a fonte entrega: **VIIRS** `confidence`
`{low,nominal,high}` (contagens) e **MODIS** `confidenceMean` (média 0–100, ponderada pelas detecções). Não há mediana de
FRP. Mais `notice` (contagens de MODIS e VIIRS não são diretamente comparáveis) e `meta`.

### `GET /compare/periods` — comparação entre dois períodos
Parâmetros: `cells`, `startA`, `endA`, `startB`, `endB`, `sensor`. Por sensor: `periodA` e `periodB` (`detections`, `frpSum`,
`days`, `daysWithData`, `detectionsPerDay`, `frpPerDay`), `detectionsPerDayChangePct` e `frpPerDayChangePct` (variação de B
em relação a A, em %), `sufficient` e `note`.
**Sem registros em um dos períodos, `sufficient=false` e as variações não são calculadas** (RN05). Dia sem registro conta como zero.

### `GET /compare/historical` — período contra o histórico
Parâmetros: `cells`, `start`, `end`, `sensor`, `years` (1–10, padrão 5). Compara as detecções/dia do período com as **mesmas
datas** dos `years` anos anteriores. Por sensor: `current`, `baseline` (um item por ano com registro), `yearsWithData`,
`meanDetectionsPerDay`, `stdDevDetectionsPerDay`, `changeVsMeanPct`, `zScore`, `anomalous` (|z| ≥ 2), `sufficient`, `note`.
**Só calcula com 3 ou mais anos com registro** (senão `sufficient=false` e nada é inventado) — RN05. A gold tem dados desde
2000; o banco local só tem o que foi importado (`/meta/coverage`).

### `GET /timeseries` — série diária e dias atípicos
Parâmetros: `cells`, `start`, `end`, `sensor`. Por dia e sensor: `count`, `frpSum`, `robustZ` e `unusual` (z-score robusto,
mediana/MAD, limiar 3,5). `robustZ` só existe com 7 ou mais dias e variação não nula. Sem baseline sazonal.

### `GET /rankings/cells` — ranking e regiões críticas
Parâmetros: `cells`, `start`, `end`, `sensor`, `metric` (`count` padrão ou `frp`), `resolution` (0 a 6; padrão 4), `limit`
(1–200, padrão 20), `criticalPercentile` (50–99, padrão 95).
Resposta: `items` `{rank, h3, resolution, detections, frpSum, frpMax, modisDetections, viirsDetections, critical}`,
`totalCells`, `criticalThreshold`.
**Região crítica** = célula com contagem **estritamente maior** que o percentil pedido entre todas as células da região no
período. Com menos de 20 células com registro, `criticalThreshold` não é calculado e nenhuma célula é marcada (RN05).

### Metadados
| Endpoint | O que devolve |
|---|---|
| `GET /meta/sensors` | MODIS (1000 m) e VIIRS (375 m) e os satélites que os carregam |
| `GET /meta/coverage` | período e total de detecções por sensor, do que está importado |
| `GET /meta/sources` | importações da gold (concluídas e falhas): partição e versão do Delta, período, linhas lidas/gravadas/rejeitadas, linhas esperadas, `verified` |

## Ainda não disponível
- **Pontos individuais, correspondências e harmonização MODIS × VIIRS**: a gold não tem focos individuais (ficam no app principal, a partir da prata).
- **Mediana de FRP**: a gold traz soma, média e máximo.
