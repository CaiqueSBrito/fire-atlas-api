# Contrato da API (v1)

Base: `/api/v1`. JSON. Sem login. Documentação interativa: `/swagger-ui.html` (OpenAPI em `/v3/api-docs`).

## Convenções

| Item | Regra |
|---|---|
| **Região** | `bbox=minLon,minLat,maxLon,maxLat` **ou** `cells=<índices H3 em hexadecimal, separados por vírgula>` (nunca os dois; até 2000 células) |
| **Período** | `start` e `end` no formato `YYYY-MM-DD` (inclusivos) |
| **Sensor** | `sensor=MODIS`, `VIIRS` ou `BOTH` (padrão) |
| **H3** | A API **só consome H3**: não converte `bbox` em células nem célula em coordenadas. Consultas agregadas **exigem `cells`**; o front obtém as células da área visível com `h3-js` (`polygonToCells`) |
| **Sem dados** | Resposta `200` vazia com `message` ("Nenhum foco de calor encontrado..."), sem números inventados |
| **Cache** | `Cache-Control: public, max-age=60` (dados recentes) ou `600` (históricos) |

### Quando usar `bbox` e quando usar `cells`

| Endpoint | `bbox` | `cells` |
|---|---|---|
| `/detections` com `zoom` ≥ 9 (pontos) | ✅ | ✅ |
| `/detections/count`, `/compare`, `/harmonized`, `/harmonized/matches` | ✅ | ✅ |
| `/detections` com `zoom` < 9 (grade H3), `/calendar`, `/timeseries`, `/compare/periods`, `/compare/historical`, `/rankings/cells` | ❌ (400) | ✅ **obrigatório** |

### Camadas distinguíveis (RN02)
Cada ponto traz `sensor`, `satellite`, `instrument`, `product` e `sourceFile`; as células agregadas e os calendários separam MODIS e VIIRS; os resultados da harmonização (pareamento MODIS × VIIRS) têm endpoint e formato próprios (`/harmonized`).

### Identificação de indicadores (RN04)
Respostas com estatísticas trazem `meta`: `region` e período (**recorte**), `source` (**fonte**), `units` (**unidades**: detecções, FRP em MW, resolução em m) e `method` (**método de cálculo**).

### Erros (RN06, CA04)
`400` no formato RFC 7807, com **cada parâmetro rejeitado**:
```json
{
  "title": "Parametros invalidos",
  "status": 400,
  "detail": "bbox deve ter 4 valores: minLon,minLat,maxLon,maxLat; endA deve ser igual ou posterior a startA",
  "errors": [
    {"parameter": "bbox", "message": "bbox deve ter 4 valores: minLon,minLat,maxLon,maxLat"},
    {"parameter": "endA", "message": "endA deve ser igual ou posterior a startA"}
  ]
}
```
`503` = banco indisponível (mensagem genérica, sem dados de conexão); `500` = erro interno genérico.

## Endpoints

### `GET /detections` — focos
Parâmetros: região, `start`, `end`, `sensor`, `zoom` (0–22, padrão 6).
- `zoom` ≥ 9 → `mode: "points"`: até 5000 pontos (`truncated` indica corte), cada um com `lat`, `lon`, `acquiredAt`, `frp` (MW), `confidence` (l/n/h), `dayNight`, `brightness` (K), `fireType`, `product`, `sourceFile`.
- `zoom` < 9 → `mode: "grid"`: células H3 `{sensor, h3, resolution, count, frpSum, frpMax}` (sem coordenadas).

### `GET /detections/count` — contagem exata
Parâmetros: região, `start`, `end`, `sensor`. Devolve `total` e, por sensor, `detections`, `frpSum` (MW) e `daysWithData`, mais `meta`. Sem registros: `total=0`, `sensors` vazio e `message`.

### `GET /calendar` — cartas por dia
Parâmetros: `cells`, `month=YYYY-MM`, `sensor`. Devolve um item por dia do mês: `{date, modis:{count,frpSum}, viirs:{count,frpSum}}`, `daysWithFire` e `resolution`.

### `GET /compare` — comparação entre sensores
Parâmetros: região, `start`, `end`, `sensor`. Por sensor: `detections`, `daysWithData`, `frpMean`, `frpMedian`, `frpMax`, `frpTotal` (MW), `confidence` {low,nominal,high}, `nominalResolutionMeters`; mais `notice` (contagens de MODIS e VIIRS não são diretamente comparáveis) e `meta`.

### `GET /compare/periods` — comparação entre dois períodos
Parâmetros: `cells`, `startA`, `endA`, `startB`, `endB`, `sensor`. Por sensor: `periodA` e `periodB` (`detections`, `frpSum`, `days`, `daysWithData`, `detectionsPerDay`, `frpPerDay`), `detectionsPerDayChangePct` e `frpPerDayChangePct` (variação de B em relação a A, em %), `sufficient` e `note`.
**Sem registros em um dos períodos, `sufficient=false` e as variações não são calculadas** (RN05). Dia sem registro conta como zero.

### `GET /compare/historical` — período contra o histórico
Parâmetros: `cells`, `start`, `end`, `sensor`, `years` (1–10, padrão 5). Compara as detecções/dia do período com as **mesmas datas** dos `years` anos anteriores. Por sensor: `current`, `baseline` (um item por ano com registro), `yearsWithData`, `meanDetectionsPerDay`, `stdDevDetectionsPerDay`, `changeVsMeanPct`, `zScore`, `anomalous` (|z| ≥ 2), `sufficient`, `note`.
**Só calcula com 3 ou mais anos com registro** (senão `sufficient=false` e nada é inventado) — RN05.

### `GET /timeseries` — série diária e dias atípicos
Parâmetros: `cells`, `start`, `end`, `sensor`. Por dia e sensor: `count`, `frpSum`, `robustZ` e `unusual` (z-score robusto, mediana/MAD, limiar 3,5). `robustZ` só existe com 7 ou mais dias e variação não nula. Sem baseline sazonal.

### `GET /rankings/cells` — ranking e regiões críticas
Parâmetros: `cells`, `start`, `end`, `sensor`, `metric` (`count` padrão ou `frp`), `resolution` (0 até a resolução-base; padrão 4), `limit` (1–200, padrão 20), `criticalPercentile` (50–99, padrão 95).
Resposta: `items` `{rank, h3, resolution, detections, frpSum, frpMax, modisDetections, viirsDetections, critical}`, `totalCells`, `criticalThreshold`.
**Região crítica** = célula com contagem **estritamente maior** que o percentil pedido entre todas as células da região no período. Com menos de 20 células com registro, `criticalThreshold` não é calculado e nenhuma célula é marcada (RN05).

### `GET /harmonized` — harmonização MODIS × VIIRS (método v0)
Parâmetros: região, `start`, `end` (até 31 dias), `maxDistanceMeters` (100–5000, padrão 750), `maxMinutes` (1–720, padrão 180), `modisSampleCap` (100–100000, padrão 20000). Devolve taxa de correspondência, razão e correlação de FRP, `method` e `limitations`. Ver `docs/HARMONIZACAO.md`.

### `GET /harmonized/matches` — camada de correspondências
Parâmetros de `/harmonized` + `limit` (1–5000, padrão 500). Cada item traz o lado `modis` e o lado `viirs` (`sensor`, `satellite`, `lat`, `lon`, `acquiredAt`, `frp`, `sourceFile`), `distanceMeters` e `minutesApart`, do par mais próximo ao mais distante (VIIRS mais próximo de cada MODIS). `truncated` indica corte por `limit`; sem pares, `count=0` e `message`.

### `GET|PUT /preferences` — região favorita (perfil anônimo)
Cookie `fa_uid` (HttpOnly). Corpo: `{name, bbox, cells, sensor, zoom, center}`. `GET` sem perfil → `204`.

### Metadados
| Endpoint | O que devolve |
|---|---|
| `GET /meta/sensors` | MODIS (1000 m) e VIIRS (375 m) e os satélites que os carregam |
| `GET /meta/coverage` | período e total de detecções por sensor |
| `GET /meta/sources` | importações do data lake (concluídas e falhas): região, período, linhas lidas/gravadas/rejeitadas, linhas esperadas, `verified` |

## Ainda não disponível
- **Filtro por continente, país e cidade**: exige dado geográfico (limites administrativos) que hoje não existe no banco nem no data lake. Hoje a região é por coordenadas (`bbox`) ou células H3.
- **Média histórica real**: o endpoint `/compare/historical` já existe, mas só devolve média/anomalia quando há 3+ anos carregados; hoje o banco tem cerca de uma semana.
