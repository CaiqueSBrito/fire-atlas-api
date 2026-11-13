package com.fireatlas.detection.entity;

import java.util.Map;

/** Estatisticas de um sensor numa regiao/periodo (FRP em MW; confianca por classe l/n/h). */
public record SensorStats(String sensor, long detections, int daysWithData, Double frpMean, Double frpMedian,
                          Float frpMax, double frpTotal, Map<String, Long> confidence,
                          int nominalResolutionMeters) {}
