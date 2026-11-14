package com.fireatlas.api;

import com.fasterxml.jackson.annotation.JsonInclude;

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

    public record HarmonizationResponse(
            String bbox,
            LocalDate start,
            LocalDate end,
            Params params,
            Result result,
            Method method,
            String message,
            String exploratoryMethodVersion,
            ScientificState scientificState,
            DescriptiveSummary summary,
            List<CandidateAssociation> associations,
            Map<String, IndicatorStatus> unavailableIndicators
    ) {
        public record Params(
                int maxDistanceMeters,
                int maxMinutes,
                int modisSampleCap,
                boolean includeAssociations,
                int associationsLimit
        ) {}

        public record Result(
                long modisEvaluated,
                long modisWithViirsMatch,
                @JsonInclude(JsonInclude.Include.ALWAYS) Double matchRate,
                @JsonInclude(JsonInclude.Include.ALWAYS) Double meanModisFrp,
                @JsonInclude(JsonInclude.Include.ALWAYS) Double meanViirsFrpSum,
                @JsonInclude(JsonInclude.Include.ALWAYS) Double viirsToModisFrpRatio,
                @JsonInclude(JsonInclude.Include.ALWAYS) Double frpCorrelation,
                @JsonInclude(JsonInclude.Include.ALWAYS) Double medianViirsPixelsPerModis,
                boolean sampled
        ) {}

        public record Method(String name, String description, List<String> limitations) {}

        public record ScientificState(
                String correspondenceMode,
                String harmonizationStatus,
                boolean scientificallyApproved,
                String derivedClassificationStatus,
                String confidenceSemantics,
                String dataAvailabilityStatus,
                List<String> reasons
        ) {}

        public record DescriptiveSummary(
                long modisTotalInWindow,
                long viirsTotalInWindow,
                long modisEvaluated,
                boolean modisSampled,
                long modisWithCandidateMatch,
                long distinctViirsMatched,
                long candidateAssociationsTotal,
                long associationsReturned,
                boolean associationsTruncated,
                long modisEvaluatedWithFrp,
                long modisEvaluatedWithoutFrp,
                long distinctViirsMatchedWithFrp,
                long distinctViirsMatchedWithoutFrp,
                boolean exclusiveAssignmentApplied
        ) {}

        public record CandidateAssociation(
                String associationType,
                double distanceMeters,
                long timeOffsetSeconds,
                double absTimeDiffMinutes,
                DetectionObservation modis,
                DetectionObservation viirs
        ) {}

        /**
         * Observacao individual preservada com eixos completos de rastreabilidade:
         * (1) identificador na base ({@code id}, {@code acquiredDate}, {@code observationKey});
         * (2) localizador e hash integral da linha bruta ({@code sourceLineNumber}, {@code sourceRawLocator}, {@code sourceRowHash});
         * (3) assinatura semantica verificada ({@code sourceSemanticHash}), status ({@code CURRENT}, {@code UNKNOWN},
         *     {@code LEGACY_UNVERIFIED}, {@code UPDATED_FROM_LEGACY}), versao ({@code sourceSemanticHashVersion}) e
         *     assinatura historica preservada ({@code legacySourceSemanticHash}, sem expor hash nao verificado em {@code sourceSemanticHash});
         * (4) checksum integral da revisao do arquivo ({@code sourceFileSha256}, quando lido ate EOF).
         * {@code sourceRecordKey} e um rotulo compacto com prefixos de 12 caracteres para inspecao humana
         * e NAO garante unicidade criptografica universal; comparacoes exatas devem usar os hashes integrais.
         */
        public record DetectionObservation(
                long id,
                LocalDate acquiredDate,
                String observationKey,
                @JsonInclude(JsonInclude.Include.ALWAYS) Long sourceLineNumber,
                @JsonInclude(JsonInclude.Include.ALWAYS) String sourceRecordKey,
                String sensor,
                String satellite,
                String sourceProduct,
                @JsonInclude(JsonInclude.Include.ALWAYS) String version,
                String sourceFile,
                Instant acquiredAt,
                double lat,
                double lon,
                @JsonInclude(JsonInclude.Include.ALWAYS) Float frp,
                String frpUnit,
                @JsonInclude(JsonInclude.Include.ALWAYS) String confidenceRaw,
                @JsonInclude(JsonInclude.Include.ALWAYS) String confidenceClass,
                String confidenceSemantics,
                @JsonInclude(JsonInclude.Include.ALWAYS) String dayNight,
                @JsonInclude(JsonInclude.Include.ALWAYS) String sourceRawLocator,
                @JsonInclude(JsonInclude.Include.ALWAYS) String sourceRowHash,
                @JsonInclude(JsonInclude.Include.ALWAYS) String sourceSemanticHash,
                @JsonInclude(JsonInclude.Include.ALWAYS) String sourceFileSha256,
                @JsonInclude(JsonInclude.Include.ALWAYS) String sourceSemanticHashStatus,
                @JsonInclude(JsonInclude.Include.ALWAYS) String sourceSemanticHashVersion,
                @JsonInclude(JsonInclude.Include.ALWAYS) String legacySourceSemanticHash
        ) {
            public DetectionObservation(
                    long id,
                    LocalDate acquiredDate,
                    String observationKey,
                    Long sourceLineNumber,
                    String sourceRecordKey,
                    String sensor,
                    String satellite,
                    String sourceProduct,
                    String version,
                    String sourceFile,
                    Instant acquiredAt,
                    double lat,
                    double lon,
                    Float frp,
                    String frpUnit,
                    String confidenceRaw,
                    String confidenceClass,
                    String confidenceSemantics,
                    String dayNight,
                    String sourceRawLocator,
                    String sourceRowHash,
                    String sourceSemanticHash,
                    String sourceFileSha256) {
                this(id, acquiredDate, observationKey, sourceLineNumber, sourceRecordKey,
                        sensor, satellite, sourceProduct, version, sourceFile, acquiredAt, lat, lon,
                        frp, frpUnit, confidenceRaw, confidenceClass, confidenceSemantics, dayNight,
                        sourceRawLocator, sourceRowHash, sourceSemanticHash, sourceFileSha256,
                        sourceSemanticHash != null ? "CURRENT" : "UNKNOWN",
                        sourceSemanticHash != null ? "v2-ieee754-direct" : null,
                        null);
            }

            public DetectionObservation(
                    long id,
                    LocalDate acquiredDate,
                    String observationKey,
                    Long sourceLineNumber,
                    String sourceRecordKey,
                    String sensor,
                    String satellite,
                    String sourceProduct,
                    String version,
                    String sourceFile,
                    Instant acquiredAt,
                    double lat,
                    double lon,
                    Float frp,
                    String frpUnit,
                    String confidenceRaw,
                    String confidenceClass,
                    String confidenceSemantics,
                    String dayNight) {
                this(id, acquiredDate, observationKey, sourceLineNumber, sourceRecordKey,
                        sensor, satellite, sourceProduct, version, sourceFile, acquiredAt, lat, lon,
                        frp, frpUnit, confidenceRaw, confidenceClass, confidenceSemantics, dayNight,
                        null, null, null, null, "UNKNOWN", null, null);
            }
        }

        public record IndicatorStatus(String status, String reason) {}
    }

    public record SourceInfo(String product, String sensor, String sourceKey, String bbox, LocalDate periodStart,
                             LocalDate periodEnd, long rowsLoaded, LocalDate minDate, LocalDate maxDate,
                             Instant loadedAt) {}

    public record Coverage(String sensor, LocalDate minDate, LocalDate maxDate, long detections) {}

    public record Preferences(String name, List<Double> bbox, String sensor, Integer zoom, List<Double> center) {}

    public record PredictionResponse(
            boolean available,
            String status,
            String reason,
            RequestWindow request,
            @JsonInclude(JsonInclude.Include.ALWAYS) String predictionTarget,
            @JsonInclude(JsonInclude.Include.ALWAYS) ValidatedHorizon validatedHorizon,
            @JsonInclude(JsonInclude.Include.ALWAYS) String modelVersion,
            @JsonInclude(JsonInclude.Include.ALWAYS) Map<String, Object> evaluationMetrics,
            @JsonInclude(JsonInclude.Include.ALWAYS) List<PredictionPoint> predictions
    ) {
        public record RequestWindow(String bbox, LocalDate start, LocalDate end) {}

        public record ValidatedHorizon(
                String unit,
                int minLead,
                int maxLead,
                String temporalResolution,
                String spatialResolution
        ) {}

        public record PredictionPoint(
                double lat,
                double lon,
                Instant forecastTimestamp,
                Double occurrenceProbability,
                Double predictedFrpMw,
                String frpUnit,
                String confidenceLevel
        ) {
            public PredictionPoint(
                    double lat,
                    double lon,
                    Instant forecastTimestamp,
                    Double occurrenceProbability,
                    Double predictedFrpMw,
                    String confidenceLevel) {
                this(lat, lon, forecastTimestamp, occurrenceProbability, predictedFrpMw,
                        predictedFrpMw != null ? "MW" : null, confidenceLevel);
            }
        }
    }
}
