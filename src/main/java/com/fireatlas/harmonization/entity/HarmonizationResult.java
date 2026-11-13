package com.fireatlas.harmonization.entity;

/** Resultado do pareamento espaco-temporal MODIS -> VIIRS (derivado; nao altera as deteccoes originais). */
public record HarmonizationResult(long modisEvaluated, long modisWithViirsMatch, Double matchRate,
                                  Double meanModisFrp, Double meanViirsFrpSum, Double viirsToModisFrpRatio,
                                  Double frpCorrelation, Double medianViirsPixelsPerModis, boolean sampled) {}
