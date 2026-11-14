package com.fireatlas.analysis.dto;

import com.fireatlas.common.dto.IndicatorMeta;
import com.fireatlas.detection.entity.SensorStats;

import java.time.LocalDate;
import java.util.List;

/** Contratos de resposta das analises: comparacao entre sensores, serie temporal e comparacao entre periodos. */
public final class AnalysisDto {

    private AnalysisDto() {}

    public record CompareResponse(String region, LocalDate start, LocalDate end, List<SensorStats> sensors,
                                  String notice, IndicatorMeta meta, String message) {}

    public record TimeseriesPoint(LocalDate date, String sensor, int count, double frpSum, Double robustZ,
                                  boolean unusual) {}

    public record TimeseriesResponse(String method, double threshold, int resolution,
                                     List<TimeseriesPoint> series, IndicatorMeta meta, String message) {}

    /** Totais de um periodo; os valores "por dia" permitem comparar periodos de tamanhos diferentes. */
    public record PeriodStats(LocalDate start, LocalDate end, int days, int daysWithData, long detections,
                              double frpSum, Double detectionsPerDay, Double frpPerDay) {}

    /**
     * Comparacao de um sensor entre dois periodos. sufficient = ha registros nos dois periodos; sem isso as
     * variacoes ficam nulas (nunca numeros inventados) e note explica o motivo.
     */
    public record SensorPeriodComparison(String sensor, PeriodStats periodA, PeriodStats periodB,
                                         Double detectionsPerDayChangePct, Double frpPerDayChangePct,
                                         boolean sufficient, String note) {}

    public record PeriodsResponse(int resolution, List<SensorPeriodComparison> sensors, IndicatorMeta meta,
                                  String message) {}

    /** Um ano anterior da janela historica (so entram anos com registro). */
    public record YearValue(int year, LocalDate start, LocalDate end, long detections, double detectionsPerDay) {}

    public record SensorHistorical(String sensor, PeriodStats current, List<YearValue> baseline, int yearsWithData,
                                   Double meanDetectionsPerDay, Double stdDevDetectionsPerDay,
                                   Double changeVsMeanPct, Double zScore, Boolean anomalous, boolean sufficient,
                                   String note) {}

    public record HistoricalResponse(int resolution, int yearsRequested, int minYears, double zThreshold,
                                     List<SensorHistorical> sensors, IndicatorMeta meta, String message) {}
}
