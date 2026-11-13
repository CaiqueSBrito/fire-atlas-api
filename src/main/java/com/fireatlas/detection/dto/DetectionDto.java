package com.fireatlas.detection.dto;


import com.fireatlas.common.dto.IndicatorMeta;

import java.time.Instant;
import java.util.List;

/** Contratos de resposta das deteccoes (mapa). */
public final class DetectionDto {

    private DetectionDto() {}

    /** Ponto com sensor e origem (CA03): produto e arquivo do data lake de onde o registro foi importado. */
    public record Point(String sensor, String satellite, String instrument, Instant acquiredAt, double lat,
                        double lon, Float frp, String confidence, String dayNight, Float brightness,
                        Integer fireType, String product, String sourceFile) {}

    /** Celula H3 agregada: h3 em hexadecimal e resolution 3..8 (sem coordenadas: o cliente desenha pelo H3). */
    public record Cell(String sensor, String h3, int resolution, int count, double frpSum, Float frpMax) {}


    /** Contagem exata de um sensor no recorte (FRP em MW; daysWithData = dias com ao menos uma deteccao). */
    public record SensorCount(String sensor, long detections, double frpSum, int daysWithData) {}

    public record CountResponse(long total, List<SensorCount> sensors, IndicatorMeta meta, String message) {}

    /** mode = "points" (zoom proximo) ou "grid" (zoom afastado, celulas H3 na resolucao indicada). */
    public record DetectionsResponse(String mode, int count, boolean truncated, Integer resolution,
                                     List<Point> points, List<Cell> cells, String message) {}
}
