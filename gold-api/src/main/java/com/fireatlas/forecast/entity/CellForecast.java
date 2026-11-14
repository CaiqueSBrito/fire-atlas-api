package com.fireatlas.forecast.entity;

/**
 * Previsao de uma celula H3 de resolucao 6 numa data de emissao, como o pipeline de dados a gerou. Os campos
 * numericos podem faltar (nulo): nada e preenchido nem inventado.
 */
public record CellForecast(long h3, Long h3Region, Double probability7d, Double detections24h, Double detections3d,
                           Double detections7d, Double frpMean7d, Double modisBrightnessMean7d,
                           Double viirsBrightnessMean7d, Double ring1Detections24h, Double ring1Detections7d,
                           Double ring2Detections7d, Boolean active) {}
