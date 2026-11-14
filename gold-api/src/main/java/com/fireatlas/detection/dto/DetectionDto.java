package com.fireatlas.detection.dto;

import com.fireatlas.common.dto.IndicatorMeta;

import java.util.List;

/** Contratos de resposta das deteccoes (mapa e contagem). */
public final class DetectionDto {

    private DetectionDto() {}

    /** Celula H3 agregada: h3 em hexadecimal e resolution 0..6 (sem coordenadas: o cliente desenha pelo H3). */
    public record Cell(String sensor, String h3, int resolution, int count, double frpSum, Float frpMax) {}

    /** Contagem de um sensor no recorte (FRP em MW; daysWithData = dias com ao menos uma deteccao). */
    public record SensorCount(String sensor, long detections, double frpSum, int daysWithData) {}

    public record CountResponse(long total, List<SensorCount> sensors, IndicatorMeta meta, String message) {}

    /** Grade de celulas H3 na resolucao indicada (a gold nao tem pontos individuais). */
    public record DetectionsResponse(String mode, int count, boolean truncated, Integer resolution,
                                     List<Cell> cells, String message) {}
}
