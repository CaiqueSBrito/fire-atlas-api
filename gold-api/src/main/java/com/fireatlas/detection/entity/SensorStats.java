package com.fireatlas.detection.entity;

import java.util.Map;

/**
 * Estatisticas de um sensor numa regiao/periodo (FRP em MW). Confianca: no VIIRS, contagem por classe l/n/h; no
 * MODIS, a media da confianca (0-100), como a fonte entrega. Nao ha mediana de FRP: a gold so tem agregados.
 */
public record SensorStats(String sensor, long detections, int daysWithData, Double frpMean, Float frpMax,
                          double frpTotal, Map<String, Long> confidence, Double confidenceMean,
                          int nominalResolutionMeters) {}
