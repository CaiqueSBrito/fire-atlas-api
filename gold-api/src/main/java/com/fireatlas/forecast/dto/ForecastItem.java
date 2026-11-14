package com.fireatlas.forecast.dto;

/**
 * Previsao de uma celula H3 (h3 em hexadecimal, resolucao 6; region = a celula de resolucao 1 que a contem).
 * probability7d: probabilidade (0 a 1) de fogo na celula nos proximos 7 dias, calculada pelo pipeline de dados.
 */
public record ForecastItem(String h3, String region, double probability7d, Double detections24h,
                           Double detections3d, Double detections7d, Double frpMean7d,
                           Double modisBrightnessMean7d, Double viirsBrightnessMean7d, Double ring1Detections24h,
                           Double ring1Detections7d, Double ring2Detections7d, Boolean active) {}
