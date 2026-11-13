package com.fireatlas.api;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

/** Contratos de resposta da API. */
public final class Dto {

    private Dto() {}

    public record Point(String sensor, String satellite, Instant acquiredAt, double lat, double lon,
                        Float frp, String confidence, String dayNight, Float brightness) {}

    /** Celula H3 agregada: h3 em hexadecimal, resolution 3..8, lat/lon do centro. */
    public record Cell(String sensor, String h3, int resolution, double lat, double lon, int count,
                       double frpSum, Float frpMax) {}

    /** mode = "points" (zoom proximo) ou "grid" (zoom afastado, celulas H3 na resolucao indicada). */
    public record DetectionsResponse(String mode, int count, boolean truncated, Integer resolution,
                                     List<Point> points, List<Cell> cells, String message) {}

    public record SensorDay(int count, double frpSum) {}

    public record CalendarDay(LocalDate date, SensorDay modis, SensorDay viirs) {}

    public record CalendarResponse(String month, int resolution, int daysWithFire, List<CalendarDay> days,
                                   String message) {}

    public record SensorStats(String sensor, long detections, int daysWithData, Double frpMean, Double frpMedian,
                              Float frpMax, double frpTotal, Map<String, Long> confidence,
                              int nominalResolutionMeters) {}

    public record CompareResponse(String region, LocalDate start, LocalDate end, List<SensorStats> sensors,
                                  String notice, String message) {}

    public record TimeseriesPoint(LocalDate date, String sensor, int count, double frpSum, Double robustZ,
                                  boolean unusual) {}

    public record TimeseriesResponse(String method, double threshold, int resolution,
                                     List<TimeseriesPoint> series, String message) {}

    public record HarmonizationResponse(String region, LocalDate start, LocalDate end, Params params,
                                        Result result, Method method, String message) {
        public record Params(int maxDistanceMeters, int maxMinutes, int modisSampleCap) {}

        public record Result(long modisEvaluated, long modisWithViirsMatch, Double matchRate,
                             Double meanModisFrp, Double meanViirsFrpSum, Double viirsToModisFrpRatio,
                             Double frpCorrelation, Double medianViirsPixelsPerModis, boolean sampled) {}

        public record Method(String name, String description, List<String> limitations) {}
    }

    public record SourceInfo(String product, String sensor, String sourceKey, String bbox, LocalDate periodStart,
                             LocalDate periodEnd, long rowsLoaded, LocalDate minDate, LocalDate maxDate,
                             Instant loadedAt) {}

    public record Coverage(String sensor, LocalDate minDate, LocalDate maxDate, long detections) {}

    /** Entrada do catalogo: sensor, resolucao nominal e satelites que o carregam. */
    public record SensorCatalogEntry(String code, String name, int nominalResolutionMeters, String description,
                                     List<com.fireatlas.domain.Satellite> satellites) {}

    /** Regiao favorita: bbox (minLon,minLat,maxLon,maxLat) e/ou celulas H3 em hexadecimal. */
    public record Preferences(String name, List<Double> bbox, List<String> cells, String sensor, Integer zoom,
                              List<Double> center) {}
}
