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

    public record Cell(String sensor, double lat, double lon, int count, double frpSum, Float frpMax) {}

    /** mode = "points" (zoom proximo) ou "grid" (zoom afastado). */
    public record DetectionsResponse(String mode, int count, boolean truncated, Double cellSizeDeg,
                                     List<Point> points, List<Cell> cells, String message) {}

    public record SensorDay(int count, double frpSum) {}

    public record CalendarDay(LocalDate date, SensorDay modis, SensorDay viirs) {}

    public record CalendarResponse(String month, int daysWithFire, List<CalendarDay> days, String message) {}

    public record SensorStats(String sensor, long detections, int daysWithData, Double frpMean, Double frpMedian,
                              Float frpMax, double frpTotal, Map<String, Long> confidence,
                              int nominalResolutionMeters) {}

    public record CompareResponse(String bbox, LocalDate start, LocalDate end, List<SensorStats> sensors,
                                  String notice, String message) {}

    public record TimeseriesPoint(LocalDate date, String sensor, int count, double frpSum, Double robustZ,
                                  boolean unusual) {}

    public record TimeseriesResponse(String method, double threshold, List<TimeseriesPoint> series,
                                     String message) {}

    public record HarmonizationResponse(String bbox, LocalDate start, LocalDate end, Params params,
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

    public record Preferences(String name, List<Double> bbox, String sensor, Integer zoom, List<Double> center) {}
}
