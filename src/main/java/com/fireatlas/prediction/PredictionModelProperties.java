package com.fireatlas.prediction;

import com.fireatlas.api.Dto;
import com.fireatlas.domain.BBox;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.net.URI;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Configuracao e manifesto de aprovacao cientifica para habilitacao do provedor preditivo.
 *
 * <p>Por padrao {@code enabled = false}. A habilitacao ({@code enabled = true}) exige
 * manifesto completo (identidade e versao do modelo, alvo, horizonte aprovado, contrato
 * de features, artefato com SHA-256, dataset, relatorio de avaliacao fora da amostra com
 * separacao cronologica, metricas finitas e evidencia de aprovacao da equipe).
 */
@ConfigurationProperties(prefix = "fireatlas.prediction")
public class PredictionModelProperties {

    public static final String SUPPORTED_PROTOCOL_VERSION = "v1";
    private static final Pattern SHA256_HEX = Pattern.compile("^[0-9a-fA-F]{64}$");
    private static final Set<String> FORBIDDEN_SPLIT_POLICIES = Set.of(
            "RANDOM", "NONE", "UNVALIDATED", "IN_SAMPLE", "K_FOLD_RANDOM");

    private boolean enabled = false;
    private String endpointUrl;
    private String protocolVersion = SUPPORTED_PROTOCOL_VERSION;
    private int connectTimeoutMs = 2000;
    private int readTimeoutMs = 5000;
    private int maxResponseBytes = 1_048_576;
    private int maxPredictions = 5000;

    private String modelId;
    private String modelVersion;
    private String predictionTarget;
    private String featureContractId;
    private boolean supportsFrp = false;

    private String horizonUnit;
    private Integer minLead;
    private Integer maxLead;
    private String temporalResolution;
    private String spatialResolution;
    private String coverageBbox;

    private String artifactUri;
    private String artifactSha256;
    private String trainingDatasetId;
    private String evaluationReportUri;
    private String temporalSplitPolicy;
    private String baselineComparisonSummary;
    private Map<String, Double> outOfSampleMetrics = new LinkedHashMap<>();

    private String approvedBy;
    private String approvalReference;
    private Instant approvedAt;

    /**
     * Valida todos os pre-requisitos cientificos e operacionais para ativacao do provedor remoto.
     *
     * @throws IllegalStateException se qualquer requisito obrigatorio estiver ausente ou invalido
     */
    public ValidatedManifest validateForActivation() {
        if (!enabled) {
            throw new IllegalStateException("Provedor preditivo nao esta marcado como habilitado (enabled=false)");
        }
        URI uri = parseHttpUri(requireNonBlank(endpointUrl, "endpointUrl"));
        String proto = requireNonBlank(protocolVersion, "protocolVersion");
        if (!SUPPORTED_PROTOCOL_VERSION.equals(proto)) {
            throw new IllegalStateException(
                    "protocolVersion incompativel: esperado '" + SUPPORTED_PROTOCOL_VERSION + "', recebido '" + proto + "'");
        }
        if (connectTimeoutMs <= 0 || connectTimeoutMs > 60_000) {
            throw new IllegalStateException("connectTimeoutMs deve estar entre 1 e 60000");
        }
        if (readTimeoutMs <= 0 || readTimeoutMs > 60_000) {
            throw new IllegalStateException("readTimeoutMs deve estar entre 1 e 60000");
        }
        if (maxResponseBytes < 1024 || maxResponseBytes > 10_485_760) {
            throw new IllegalStateException("maxResponseBytes deve estar entre 1024 e 10485760");
        }
        if (maxPredictions < 1 || maxPredictions > 50_000) {
            throw new IllegalStateException("maxPredictions deve estar entre 1 e 50000");
        }

        String validModelId = requireNonBlank(modelId, "modelId");
        String validModelVersion = requireNonBlank(modelVersion, "modelVersion");
        String validTarget = requireNonBlank(predictionTarget, "predictionTarget");
        String validFeatureContract = requireNonBlank(featureContractId, "featureContractId");

        String validUnit = requireNonBlank(horizonUnit, "horizonUnit").toUpperCase(Locale.ROOT);
        if (!"DAYS".equals(validUnit) && !"HOURS".equals(validUnit)) {
            throw new IllegalStateException("horizonUnit deve ser DAYS ou HOURS");
        }
        if (minLead == null || minLead < 0) {
            throw new IllegalStateException("minLead obrigatorio e deve ser >= 0");
        }
        if (maxLead == null || maxLead < minLead || maxLead <= 0) {
            throw new IllegalStateException("maxLead obrigatorio e deve ser >= minLead e > 0");
        }
        int maxAllowedLead = "DAYS".equals(validUnit) ? 31 : 31 * 24;
        if (maxLead > maxAllowedLead) {
            throw new IllegalStateException("maxLead excede o limite operacional permitido (" + maxAllowedLead + " " + validUnit + ")");
        }
        String validTemporalRes = requireNonBlank(temporalResolution, "temporalResolution");
        String validSpatialRes = requireNonBlank(spatialResolution, "spatialResolution");

        BBox parsedCoverageBbox;
        try {
            parsedCoverageBbox = BBox.parse(requireNonBlank(coverageBbox, "coverageBbox"));
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("coverageBbox invalido no manifesto do modelo: " + e.getMessage(), e);
        }

        String validArtifactUri = requireNonBlank(artifactUri, "artifactUri");
        String validSha = requireNonBlank(artifactSha256, "artifactSha256");
        if (!SHA256_HEX.matcher(validSha).matches()) {
            throw new IllegalStateException("artifactSha256 deve conter 64 caracteres hexadecimais (SHA-256)");
        }
        String validDatasetId = requireNonBlank(trainingDatasetId, "trainingDatasetId");
        String validEvalUri = requireNonBlank(evaluationReportUri, "evaluationReportUri");
        String validSplitPolicy = requireNonBlank(temporalSplitPolicy, "temporalSplitPolicy");
        if (FORBIDDEN_SPLIT_POLICIES.contains(validSplitPolicy.toUpperCase(Locale.ROOT))) {
            throw new IllegalStateException(
                    "temporalSplitPolicy invalida ('" + validSplitPolicy + "'): exigida separacao cronologica sem vazamento temporal");
        }
        String validBaselineSummary = requireNonBlank(baselineComparisonSummary, "baselineComparisonSummary");

        if (outOfSampleMetrics == null || outOfSampleMetrics.isEmpty()) {
            throw new IllegalStateException("outOfSampleMetrics obrigatorio: metricas de teste fora da amostra devem estar registradas");
        }
        Map<String, Object> sanitizedMetrics = new LinkedHashMap<>();
        for (Map.Entry<String, Double> entry : outOfSampleMetrics.entrySet()) {
            String key = requireNonBlank(entry.getKey(), "outOfSampleMetrics.key");
            Double val = entry.getValue();
            if (val == null || !Double.isFinite(val)) {
                throw new IllegalStateException("outOfSampleMetrics['" + key + "'] deve ser um numero finito");
            }
            sanitizedMetrics.put(key, val);
        }

        String validApprovedBy = requireNonBlank(approvedBy, "approvedBy");
        String validApprovalRef = requireNonBlank(approvalReference, "approvalReference");
        if (approvedAt == null) {
            throw new IllegalStateException("approvedAt obrigatorio no manifesto de aprovacao");
        }

        return new ValidatedManifest(
                uri,
                proto,
                connectTimeoutMs,
                readTimeoutMs,
                maxResponseBytes,
                maxPredictions,
                validModelId,
                validModelVersion,
                validTarget,
                validFeatureContract,
                supportsFrp,
                new Dto.PredictionResponse.ValidatedHorizon(
                        validUnit, minLead, maxLead, validTemporalRes, validSpatialRes),
                parsedCoverageBbox,
                validArtifactUri,
                validSha.toLowerCase(Locale.ROOT),
                validDatasetId,
                validEvalUri,
                validSplitPolicy,
                validBaselineSummary,
                Collections.unmodifiableMap(sanitizedMetrics),
                validApprovedBy,
                validApprovalRef,
                approvedAt
        );
    }

    private static String requireNonBlank(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(
                    "Pre-requisito obrigatorio ausente para ativacao do modelo preditivo: " + fieldName);
        }
        return value.trim();
    }

    private static URI parseHttpUri(String rawUrl) {
        try {
            URI uri = URI.create(rawUrl);
            String scheme = uri.getScheme();
            if (scheme == null || (!"http".equalsIgnoreCase(scheme) && !"https".equalsIgnoreCase(scheme))) {
                throw new IllegalStateException("endpointUrl deve utilizar esquema http ou https");
            }
            if (uri.getHost() == null || uri.getHost().isBlank()) {
                throw new IllegalStateException("endpointUrl deve conter host valido");
            }
            return uri;
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("endpointUrl malformada", e);
        }
    }

    public record ValidatedManifest(
            URI endpointUri,
            String protocolVersion,
            int connectTimeoutMs,
            int readTimeoutMs,
            int maxResponseBytes,
            int maxPredictions,
            String modelId,
            String modelVersion,
            String predictionTarget,
            String featureContractId,
            boolean supportsFrp,
            Dto.PredictionResponse.ValidatedHorizon validatedHorizon,
            BBox coverageBbox,
            String artifactUri,
            String artifactSha256,
            String trainingDatasetId,
            String evaluationReportUri,
            String temporalSplitPolicy,
            String baselineComparisonSummary,
            Map<String, Object> outOfSampleMetrics,
            String approvedBy,
            String approvalReference,
            Instant approvedAt
    ) {
        public Instant minHorizonInstant(Instant asOf) {
            if ("HOURS".equals(validatedHorizon.unit())) {
                return asOf.plus( java.time.Duration.ofHours(validatedHorizon.minLead()));
            }
            java.time.LocalDate minDate = java.time.LocalDate.ofInstant(asOf, java.time.ZoneOffset.UTC)
                    .plusDays(validatedHorizon.minLead());
            Instant startOfMinDate = minDate.atStartOfDay(java.time.ZoneOffset.UTC).toInstant();
            return startOfMinDate.isAfter(asOf) ? startOfMinDate : asOf;
        }

        public Instant maxHorizonInstant(Instant asOf) {
            if ("HOURS".equals(validatedHorizon.unit())) {
                return asOf.plus(java.time.Duration.ofHours(validatedHorizon.maxLead()));
            }
            java.time.LocalDate maxDate = java.time.LocalDate.ofInstant(asOf, java.time.ZoneOffset.UTC)
                    .plusDays(validatedHorizon.maxLead());
            return maxDate.plusDays(1).atStartOfDay(java.time.ZoneOffset.UTC).toInstant().minusNanos(1);
        }

        public java.time.LocalDate minAllowedDate(Instant asOf) {
            return java.time.LocalDate.ofInstant(minHorizonInstant(asOf), java.time.ZoneOffset.UTC);
        }

        public java.time.LocalDate maxAllowedDate(Instant asOf) {
            return java.time.LocalDate.ofInstant(maxHorizonInstant(asOf), java.time.ZoneOffset.UTC);
        }

        public int minLeadDays() {
            if ("DAYS".equals(validatedHorizon.unit())) {
                return validatedHorizon.minLead();
            }
            return validatedHorizon.minLead() / 24;
        }

        public int maxLeadDays() {
            if ("DAYS".equals(validatedHorizon.unit())) {
                return validatedHorizon.maxLead();
            }
            return (validatedHorizon.maxLead() + 23) / 24;
        }
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getEndpointUrl() {
        return endpointUrl;
    }

    public void setEndpointUrl(String endpointUrl) {
        this.endpointUrl = endpointUrl;
    }

    public String getProtocolVersion() {
        return protocolVersion;
    }

    public void setProtocolVersion(String protocolVersion) {
        this.protocolVersion = protocolVersion;
    }

    public int getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    public void setConnectTimeoutMs(int connectTimeoutMs) {
        this.connectTimeoutMs = connectTimeoutMs;
    }

    public int getReadTimeoutMs() {
        return readTimeoutMs;
    }

    public void setReadTimeoutMs(int readTimeoutMs) {
        this.readTimeoutMs = readTimeoutMs;
    }

    public int getMaxResponseBytes() {
        return maxResponseBytes;
    }

    public void setMaxResponseBytes(int maxResponseBytes) {
        this.maxResponseBytes = maxResponseBytes;
    }

    public int getMaxPredictions() {
        return maxPredictions;
    }

    public void setMaxPredictions(int maxPredictions) {
        this.maxPredictions = maxPredictions;
    }

    public String getModelId() {
        return modelId;
    }

    public void setModelId(String modelId) {
        this.modelId = modelId;
    }

    public String getModelVersion() {
        return modelVersion;
    }

    public void setModelVersion(String modelVersion) {
        this.modelVersion = modelVersion;
    }

    public String getPredictionTarget() {
        return predictionTarget;
    }

    public void setPredictionTarget(String predictionTarget) {
        this.predictionTarget = predictionTarget;
    }

    public String getFeatureContractId() {
        return featureContractId;
    }

    public void setFeatureContractId(String featureContractId) {
        this.featureContractId = featureContractId;
    }

    public boolean isSupportsFrp() {
        return supportsFrp;
    }

    public void setSupportsFrp(boolean supportsFrp) {
        this.supportsFrp = supportsFrp;
    }

    public String getHorizonUnit() {
        return horizonUnit;
    }

    public void setHorizonUnit(String horizonUnit) {
        this.horizonUnit = horizonUnit;
    }

    public Integer getMinLead() {
        return minLead;
    }

    public void setMinLead(Integer minLead) {
        this.minLead = minLead;
    }

    public Integer getMaxLead() {
        return maxLead;
    }

    public void setMaxLead(Integer maxLead) {
        this.maxLead = maxLead;
    }

    public String getTemporalResolution() {
        return temporalResolution;
    }

    public void setTemporalResolution(String temporalResolution) {
        this.temporalResolution = temporalResolution;
    }

    public String getSpatialResolution() {
        return spatialResolution;
    }

    public void setSpatialResolution(String spatialResolution) {
        this.spatialResolution = spatialResolution;
    }

    public String getCoverageBbox() {
        return coverageBbox;
    }

    public void setCoverageBbox(String coverageBbox) {
        this.coverageBbox = coverageBbox;
    }

    public String getArtifactUri() {
        return artifactUri;
    }

    public void setArtifactUri(String artifactUri) {
        this.artifactUri = artifactUri;
    }

    public String getArtifactSha256() {
        return artifactSha256;
    }

    public void setArtifactSha256(String artifactSha256) {
        this.artifactSha256 = artifactSha256;
    }

    public String getTrainingDatasetId() {
        return trainingDatasetId;
    }

    public void setTrainingDatasetId(String trainingDatasetId) {
        this.trainingDatasetId = trainingDatasetId;
    }

    public String getEvaluationReportUri() {
        return evaluationReportUri;
    }

    public void setEvaluationReportUri(String evaluationReportUri) {
        this.evaluationReportUri = evaluationReportUri;
    }

    public String getTemporalSplitPolicy() {
        return temporalSplitPolicy;
    }

    public void setTemporalSplitPolicy(String temporalSplitPolicy) {
        this.temporalSplitPolicy = temporalSplitPolicy;
    }

    public String getBaselineComparisonSummary() {
        return baselineComparisonSummary;
    }

    public void setBaselineComparisonSummary(String baselineComparisonSummary) {
        this.baselineComparisonSummary = baselineComparisonSummary;
    }

    public Map<String, Double> getOutOfSampleMetrics() {
        return outOfSampleMetrics;
    }

    public void setOutOfSampleMetrics(Map<String, Double> outOfSampleMetrics) {
        this.outOfSampleMetrics = outOfSampleMetrics;
    }

    public String getApprovedBy() {
        return approvedBy;
    }

    public void setApprovedBy(String approvedBy) {
        this.approvedBy = approvedBy;
    }

    public String getApprovalReference() {
        return approvalReference;
    }

    public void setApprovalReference(String approvalReference) {
        this.approvalReference = approvalReference;
    }

    public Instant getApprovedAt() {
        return approvedAt;
    }

    public void setApprovedAt(Instant approvedAt) {
        this.approvedAt = approvedAt;
    }
}
