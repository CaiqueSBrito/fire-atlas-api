# Harmonizacao MODIS × VIIRS — metodo v0 (a validar pela equipe)

> **INDISPONIVEL COM A GOLD.** A API passou a usar somente a camada ouro (`gold/map_grid`), que traz agregados por
> celula H3, dia e sensor, sem focos individuais. O pareamento abaixo exige posicao e hora de cada deteccao, por isso
> `/harmonized` e `/harmonized/matches` respondem 501. O codigo do metodo v0 esta no historico do Git (commit 1689dc3,
> branch `feature/importacao-fontes`) e pode voltar se a gold passar a ter focos individuais. O texto abaixo documenta
> o metodo para essa eventualidade.

## Problema
MODIS (~1 km) e VIIRS (375 m) detectam focos com resolucao e sensibilidade diferentes. Contar pontos de cada sensor
e comparar **nao e valido**: o VIIRS enxerga focos menores e um pixel MODIS pode conter varios pixels VIIRS.

## O que a API faz hoje

1. **Padronizacao de campos** (ingestao): esquema unico `detection` com sensor, satelite, instante UTC
   (`acq_date + acq_time`), lat/lon, brilho, FRP (MW), `confidence_raw` (valor original) e `confidence_class`
   (l/n/h harmonizada: MODIS 0–29 = l, 30–79 = n, 80–100 = h; VIIRS ja usa l/n/h). A origem
   (`source_product`, `source_file`) e preservada em toda linha.
2. **Pareamento espaco-temporal MODIS → VIIRS** (`/harmonized`): para cada deteccao MODIS (amostra limitada),
   busca deteccoes VIIRS a ate `maxDistanceMeters` (padrao 750 m ≈ meia diagonal do pixel MODIS) e
   `maxMinutes` (padrao 180 min) de diferenca.
3. **Indicadores reportados**: taxa de correspondencia; FRP medio MODIS vs soma de FRP VIIRS no mesmo pixel;
   razao `soma(FRP VIIRS)/soma(FRP MODIS)`; correlacao de Pearson do FRP; mediana de pixels VIIRS por pixel MODIS.
4. **Atividade incomum** (`/timeseries`): z-score robusto (mediana/MAD) sobre contagens diarias; limiar 3,5.

## Limitacoes conhecidas (documentar no pitch)
- Os limiares (750 m, 60 min, 3,5) sao pontos de partida; **precisam de validacao/sensibilidade** com a equipe.
- **Achado empirico (Brasil, 01-07/09/2026, 4 satelites VIIRS carregados):** as passagens MODIS e VIIRS sobre o
  mesmo local ficam longe no tempo. Aqua passa ~18/06 UTC (saiu da orbita original), Terra ~11/01 UTC e
  S-NPP/NOAA-20/NOAA-21 ~04-05 e ~16-17 UTC. Entre focos a ate 750 m, a mediana de diferenca de tempo e ~107-150 min
  (Aqua×VIIRS) e ~250-290 min (Terra×VIIRS); **nenhum par ficou a menos de 30 min**. Com a janela de 60 min a taxa de
  correspondencia era 0,27%. Por isso o pareamento instantaneo nao serve: o metodo deve comparar o "mesmo foco no mesmo
  periodo" (janela ~3 h para Aqua) ou agregar por celula/dia, e tratar Terra separadamente.
- Nuvens, angulo de varredura e tamanho do pixel afetam cada sensor de modo distinto.
- O z-score nao tem baseline sazonal (compara dentro do proprio periodo).
- A amostra MODIS e limitada (`modisSampleCap`); o campo `sampled` indica quando houve corte.

## Sugestao de validacao
Escolher uma regiao e um periodo (ex.: 1 mes), variar `maxDistanceMeters` e `maxMinutes`, e mostrar como a taxa de
correspondencia e a razao de FRP se estabilizam; comparar com a literatura de validacao MODIS/VIIRS.
