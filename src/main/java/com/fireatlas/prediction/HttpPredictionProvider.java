package com.fireatlas.prediction;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fireatlas.api.Dto;
import com.fireatlas.domain.BBox;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Adaptador HTTP para consumo de servico externo de inferencia preditiva segundo o protocolo v1.
 *
 * <p>So pode ser instanciado a partir de um {@link PredictionModelProperties.ValidatedManifest}.
 * Aplica validacao estrita de escopo, protocolo, identidade do modelo, cutoff temporal ({@code asOf}),
 * cobertura, coordenadas, probabilidades e FRP antes de expor qualquer resultado.
 */
public class HttpPredictionProvider implements PredictionProvider {

    public static final String PROTOCOL_V1 = "v1";
    public static final String COVERAGE_SUFFICIENT = "SUFFICIENT";
    public static final String COVERAGE_INSUFFICIENT = "INSUFFICIENT_COVERAGE";

    private final PredictionModelProperties.ValidatedManifest manifest;
    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;

    public HttpPredictionProvider(PredictionModelProperties.ValidatedManifest manifest, ObjectMapper objectMapper) {
        this(manifest, objectMapper, HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(Objects.requireNonNull(manifest, "manifest obrigatorio").connectTimeoutMs()))
                .followRedirects(HttpClient.Redirect.NEVER)
                .build());
    }

    public HttpPredictionProvider(PredictionModelProperties.ValidatedManifest manifest,
                                  ObjectMapper objectMapper,
                                  HttpClient httpClient) {
        this.manifest = Objects.requireNonNull(manifest, "manifest obrigatorio");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper obrigatorio");
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient obrigatorio");
    }

    public PredictionModelProperties.ValidatedManifest manifest() {
        return manifest;
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public ProviderResult predict(ProviderRequest request) {
        Objects.requireNonNull(request, "request obrigatorio");

        String scopeViolation = checkApprovedScope(request);
        if (scopeViolation != null) {
            return new ProviderResult(
                    OutcomeStatus.OUT_OF_APPROVED_SCOPE,
                    scopeViolation,
                    manifest.predictionTarget(),
                    manifest.validatedHorizon(),
                    manifest.modelVersion(),
                    manifest.outOfSampleMetrics(),
                    null
            );
        }

        V1PredictRequest wireRequest = new V1PredictRequest(
                PROTOCOL_V1,
                request.requestId(),
                new V1BBox(request.bbox().minLon(), request.bbox().minLat(),
                        request.bbox().maxLon(), request.bbox().maxLat()),
                request.asOf(),
                new V1Window(request.start(), request.end()),
                manifest.predictionTarget(),
                manifest.featureContractId(),
                manifest.modelId(),
                manifest.modelVersion()
        );

        byte[] requestBytes;
        try {
            requestBytes = objectMapper.writeValueAsBytes(wireRequest);
        } catch (IOException e) {
            throw PredictionProviderException.unavailable(
                    "Falha interna ao preparar requisicao para o provedor preditivo.",
                    "serialization error", e);
        }

        HttpRequest httpRequest = HttpRequest.newBuilder()
                .uri(manifest.endpointUri())
                .timeout(Duration.ofMillis(manifest.readTimeoutMs()))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .header("X-Request-Id", request.requestId())
                .POST(HttpRequest.BodyPublishers.ofByteArray(requestBytes))
                .build();

        byte[] responseBytes = executeHttp(httpRequest);
        V1PredictResponse wireResponse = parseWireResponse(responseBytes);
        return validateAndMap(request, wireResponse);
    }

    private String checkApprovedScope(ProviderRequest request) {
        BBox cov = manifest.coverageBbox();
        BBox req = request.bbox();
        if (req.minLon() < cov.minLon() || req.maxLon() > cov.maxLon()
                || req.minLat() < cov.minLat() || req.maxLat() > cov.maxLat()) {
            return "O recorte espacial solicitado (" + req + ") extrapola o dominio geografico aprovado do modelo ("
                    + cov + ").";
        }

        LocalDate minAllowedDate = manifest.minAllowedDate(request.asOf());
        LocalDate maxAllowedDate = manifest.maxAllowedDate(request.asOf());
        if (request.start().isBefore(minAllowedDate) || request.end().isAfter(maxAllowedDate)) {
            return "A janela solicitada (" + request.start() + " a " + request.end()
                    + ") esta fora do horizonte aprovado do modelo (" + minAllowedDate + " a " + maxAllowedDate
                    + " em relacao ao instante de corte " + request.asOf() + ").";
        }
        return null;
    }

    private byte[] executeHttp(HttpRequest httpRequest) {
        long timeoutMs = manifest.readTimeoutMs();
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        CompletableFuture<HttpResponse<InputStream>> responseFuture =
                httpClient.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofInputStream());

        HttpResponse<InputStream> response;
        try {
            long waitNanos = remainingNanos(deadlineNanos, timeoutMs);
            response = responseFuture.get(waitNanos, TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            responseFuture.cancel(true);
            throw PredictionProviderException.unavailable(
                    "Tempo limite esgotado ao consultar o provedor de inferencia preditiva.",
                    "header timeout after " + timeoutMs + "ms", e);
        } catch (InterruptedException e) {
            responseFuture.cancel(true);
            Thread.currentThread().interrupt();
            throw PredictionProviderException.unavailable(
                    "Consulta ao provedor de inferencia preditiva foi interrompida.",
                    "interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof HttpTimeoutException) {
                throw PredictionProviderException.unavailable(
                        "Tempo limite esgotado ao consultar o provedor de inferencia preditiva.",
                        "timeout after " + timeoutMs + "ms", cause);
            }
            throw PredictionProviderException.unavailable(
                    "Provedor de inferencia preditiva indisponivel no momento.",
                    cause.getClass().getSimpleName(), cause);
        }

        int status = response.statusCode();
        InputStream bodyStream = response.body();
        if (status != 200) {
            closeQuietly(bodyStream);
            throw PredictionProviderException.unavailable(
                    "Provedor de inferencia preditiva retornou falha operacional.",
                    "HTTP status " + status);
        }
        if (bodyStream == null) {
            throw PredictionProviderException.invalidResponse(
                    "Provedor de inferencia preditiva retornou corpo vazio.",
                    "null body stream");
        }

        AtomicBoolean timedOut = new AtomicBoolean(false);
        long bodyWaitNanos = remainingNanos(deadlineNanos, timeoutMs);
        CompletableFuture<Void> timeoutKiller = CompletableFuture.runAsync(() -> {
            timedOut.set(true);
            closeQuietly(bodyStream);
        }, CompletableFuture.delayedExecutor(bodyWaitNanos, TimeUnit.NANOSECONDS));

        CompletableFuture<byte[]> bodyReadFuture = CompletableFuture.supplyAsync(() -> {
            try (InputStream in = bodyStream) {
                return in.readNBytes(manifest.maxResponseBytes() + 1);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        try {
            byte[] bytes = bodyReadFuture.get(bodyWaitNanos, TimeUnit.NANOSECONDS);
            if (timedOut.get()) {
                throw PredictionProviderException.unavailable(
                        "Tempo limite esgotado durante leitura da resposta do provedor preditivo.",
                        "body read timeout after " + timeoutMs + "ms");
            }
            if (bytes.length == 0) {
                throw PredictionProviderException.invalidResponse(
                        "Provedor de inferencia preditiva retornou corpo vazio.",
                        "empty payload");
            }
            if (bytes.length > manifest.maxResponseBytes()) {
                throw PredictionProviderException.invalidResponse(
                        "Resposta do provedor de inferencia preditiva excedeu o limite maximo de tamanho.",
                        "payload exceeded " + manifest.maxResponseBytes() + " bytes");
            }
            return bytes;
        } catch (TimeoutException e) {
            timedOut.set(true);
            closeQuietly(bodyStream);
            bodyReadFuture.cancel(true);
            throw PredictionProviderException.unavailable(
                    "Tempo limite esgotado durante leitura da resposta do provedor preditivo.",
                    "body read timeout after " + timeoutMs + "ms", e);
        } catch (InterruptedException e) {
            closeQuietly(bodyStream);
            bodyReadFuture.cancel(true);
            Thread.currentThread().interrupt();
            throw PredictionProviderException.unavailable(
                    "Consulta ao provedor de inferencia preditiva foi interrompida.",
                    "interrupted during body read", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof UncheckedIOException uio && uio.getCause() != null) {
                cause = uio.getCause();
            }
            if (timedOut.get() || cause instanceof HttpTimeoutException) {
                throw PredictionProviderException.unavailable(
                        "Tempo limite esgotado durante leitura da resposta do provedor preditivo.",
                        "body read timeout after " + timeoutMs + "ms", cause);
            }
            throw PredictionProviderException.unavailable(
                    "Falha de comunicacao ao ler resposta do provedor preditivo.",
                    cause.getClass().getSimpleName(), cause);
        } finally {
            timeoutKiller.cancel(false);
            closeQuietly(bodyStream);
        }
    }

    private static long remainingNanos(long deadlineNanos, long timeoutMs) {
        long remaining = deadlineNanos - System.nanoTime();
        if (remaining <= 0) {
            throw PredictionProviderException.unavailable(
                    "Tempo limite esgotado ao consultar o provedor de inferencia preditiva.",
                    "deadline expired after " + timeoutMs + "ms");
        }
        return remaining;
    }

    private static void closeQuietly(InputStream stream) {
        if (stream != null) {
            try {
                stream.close();
            } catch (IOException ignored) {
            }
        }
    }

    private V1PredictResponse parseWireResponse(byte[] responseBytes) {
        try {
            V1PredictResponse parsed = objectMapper.readValue(responseBytes, V1PredictResponse.class);
            if (parsed == null) {
                throw PredictionProviderException.invalidResponse(
                        "Resposta do provedor de inferencia preditiva e invalida.",
                        "deserialized null");
            }
            return parsed;
        } catch (PredictionProviderException e) {
            throw e;
        } catch (IOException e) {
            throw PredictionProviderException.invalidResponse(
                    "Resposta do provedor de inferencia preditiva malformada ou incompativel com o protocolo v1.",
                    "json parse failure: " + e.getClass().getSimpleName(), e);
        }
    }

    private ProviderResult validateAndMap(ProviderRequest request, V1PredictResponse resp) {
        if (!PROTOCOL_V1.equals(resp.protocolVersion())) {
            throw PredictionProviderException.invalidResponse(
                    "Versao de protocolo do provedor preditivo incompativel.",
                    "expected protocol v1, got " + resp.protocolVersion());
        }
        if (resp.executionId() == null || resp.executionId().isBlank()) {
            throw PredictionProviderException.invalidResponse(
                    "Resposta do provedor preditivo sem identificador de execucao (executionId).",
                    "missing executionId");
        }
        if (!manifest.modelId().equals(resp.modelId()) || !manifest.modelVersion().equals(resp.modelVersion())) {
            throw PredictionProviderException.invalidResponse(
                    "Identidade ou versao do modelo remoto diverge do manifesto aprovado.",
                    "mismatch modelId/modelVersion");
        }
        if (!manifest.predictionTarget().equals(resp.predictionTarget())) {
            throw PredictionProviderException.invalidResponse(
                    "Variavel-alvo retornada pelo provedor diverge do manifesto aprovado.",
                    "mismatch predictionTarget");
        }
        if (resp.validatedHorizon() == null || !resp.validatedHorizon().equals(manifest.validatedHorizon())) {
            throw PredictionProviderException.invalidResponse(
                    "Horizonte validado retornado pelo provedor diverge do manifesto aprovado.",
                    "mismatch validatedHorizon");
        }
        if (resp.asOf() == null || resp.asOf().isAfter(request.asOf())
                || resp.asOf().isBefore(request.asOf().minus(Duration.ofHours(24)))) {
            throw PredictionProviderException.invalidResponse(
                    "Instante de referencia (asOf) da resposta do provedor e invalido ou posterior ao corte permitido.",
                    "invalid asOf: " + resp.asOf());
        }
        if (resp.predictedInterval() == null
                || !request.start().equals(resp.predictedInterval().start())
                || !request.end().equals(resp.predictedInterval().end())) {
            throw PredictionProviderException.invalidResponse(
                    "Intervalo previsto retornado pelo provedor diverge da janela solicitada.",
                    "mismatch predictedInterval");
        }
        if (resp.coverage() == null || resp.coverage().status() == null || resp.coverage().status().isBlank()) {
            throw PredictionProviderException.invalidResponse(
                    "Metadados de cobertura e disponibilidade ausentes na resposta do provedor.",
                    "missing coverage");
        }
        Double covRatio = resp.coverage().observedCoverageRatio();
        if (covRatio != null && (!Double.isFinite(covRatio) || covRatio < 0.0 || covRatio > 1.0)) {
            throw PredictionProviderException.invalidResponse(
                    "Indice de cobertura observacional invalido na resposta do provedor.",
                    "invalid observedCoverageRatio");
        }

        Map<String, Object> mergedMetrics = buildEvaluationMetadata(resp.evaluationSummary());

        String covStatus = resp.coverage().status();
        if (COVERAGE_INSUFFICIENT.equals(covStatus)) {
            if (resp.predictions() != null && !resp.predictions().isEmpty()) {
                throw PredictionProviderException.invalidResponse(
                        "Resposta inconsistente do provedor: predições enviadas apesar de cobertura insuficiente.",
                        "non-empty predictions with INSUFFICIENT_COVERAGE");
            }
            String covReason = (resp.coverage().reason() != null && !resp.coverage().reason().isBlank())
                    ? resp.coverage().reason().trim()
                    : "Cobertura observacional ou de variaveis insuficiente no recorte para emitir previsao confiavel; "
                            + "ausencia de cobertura nao equivale a previsao de zero ocorrencias.";
            return new ProviderResult(
                    OutcomeStatus.INSUFFICIENT_DATA_COVERAGE,
                    covReason,
                    manifest.predictionTarget(),
                    manifest.validatedHorizon(),
                    manifest.modelVersion(),
                    mergedMetrics,
                    null
            );
        }

        if (!COVERAGE_SUFFICIENT.equals(covStatus)) {
            throw PredictionProviderException.invalidResponse(
                    "Status de cobertura desconhecido retornado pelo provedor preditivo.",
                    "unknown coverage.status: " + covStatus);
        }

        List<V1PredictionItem> rawItems = resp.predictions();
        if (rawItems == null || rawItems.isEmpty()) {
            throw PredictionProviderException.invalidResponse(
                    "Resposta do provedor preditivo nao contem previsoes justificadas para cobertura suficiente.",
                    "null or empty predictions with SUFFICIENT coverage");
        }
        if (rawItems.size() > manifest.maxPredictions()) {
            throw PredictionProviderException.invalidResponse(
                    "Quantidade de previsoes retornadas pelo provedor excede o limite configurado.",
                    "predictions size " + rawItems.size() + " > " + manifest.maxPredictions());
        }

        Instant windowStart = request.start().atStartOfDay(ZoneOffset.UTC).toInstant();
        Instant windowEnd = request.end().plusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant().minusNanos(1);
        Instant minHorizonInstant = manifest.minHorizonInstant(request.asOf());
        Instant maxHorizonInstant = manifest.maxHorizonInstant(request.asOf());

        List<Dto.PredictionResponse.PredictionPoint> validatedPoints = new ArrayList<>(rawItems.size());
        for (int i = 0; i < rawItems.size(); i++) {
            V1PredictionItem item = rawItems.get(i);
            if (item == null) {
                throw PredictionProviderException.invalidResponse(
                        "Item de previsao nulo na resposta do provedor.",
                        "null item at index " + i);
            }
            if (item.lat() == null || item.lon() == null
                    || !Double.isFinite(item.lat()) || !Double.isFinite(item.lon())) {
                throw PredictionProviderException.invalidResponse(
                        "Coordenadas ausentes ou nao finitas na resposta do provedor.",
                        "invalid lat/lon at index " + i);
            }
            if (item.lat() < -90.0 || item.lat() > 90.0 || item.lon() < -180.0 || item.lon() > 180.0
                    || !request.bbox().contains(item.lon(), item.lat())) {
                throw PredictionProviderException.invalidResponse(
                        "Coordenada prevista fora do recorte espacial solicitado.",
                        "out-of-bbox coordinate at index " + i);
            }
            Instant ts = item.forecastTimestamp();
            if (ts == null || ts.isBefore(request.asOf()) || ts.isBefore(windowStart)
                    || ts.isAfter(windowEnd) || ts.isBefore(minHorizonInstant) || ts.isAfter(maxHorizonInstant)) {
                throw PredictionProviderException.invalidResponse(
                        "Timestamp previsto invalido ou fora do horizonte contratado.",
                        "invalid forecastTimestamp at index " + i);
            }

            Double prob = item.occurrenceProbability();
            Double frp = item.predictedFrpMw();
            if (prob == null && frp == null) {
                throw PredictionProviderException.invalidResponse(
                        "Item de previsao sem probabilidade nem FRP prevista.",
                        "empty prediction values at index " + i);
            }
            if (prob != null && (!Double.isFinite(prob) || prob < 0.0 || prob > 1.0)) {
                throw PredictionProviderException.invalidResponse(
                        "Probabilidade prevista fora do intervalo [0, 1] ou nao finita.",
                        "invalid occurrenceProbability at index " + i);
            }
            String frpUnit = null;
            if (frp != null) {
                if (!manifest.supportsFrp()) {
                    throw PredictionProviderException.invalidResponse(
                            "Provedor retornou FRP futura para modelo que nao declara suporte a FRP no manifesto.",
                            "unexpected predictedFrpMw at index " + i);
                }
                if (!Double.isFinite(frp) || frp < 0.0 || Double.compare(frp, -0.0d) == 0) {
                    throw PredictionProviderException.invalidResponse(
                            "FRP prevista negativa ou nao finita na resposta do provedor.",
                            "invalid predictedFrpMw at index " + i);
                }
                if (!"MW".equals(item.frpUnit())) {
                    throw PredictionProviderException.invalidResponse(
                            "FRP prevista sem unidade 'MW' declarada.",
                            "invalid frpUnit at index " + i);
                }
                frpUnit = "MW";
            } else if (item.frpUnit() != null) {
                throw PredictionProviderException.invalidResponse(
                        "Unidade de FRP informada sem valor de FRP previsto.",
                        "frpUnit without predictedFrpMw at index " + i);
            }

            String confidence = null;
            if (item.confidenceLevel() != null) {
                if (item.confidenceLevel().isBlank()) {
                    throw PredictionProviderException.invalidResponse(
                            "Nivel de confianca em branco na resposta do provedor.",
                            "blank confidenceLevel at index " + i);
                }
                confidence = item.confidenceLevel().trim();
            }

            validatedPoints.add(new Dto.PredictionResponse.PredictionPoint(
                    item.lat(), item.lon(), ts, prob, frp, frpUnit, confidence));
        }

        String okReason = "Previsao gerada pelo modelo aprovado " + manifest.modelId() + " (" + manifest.modelVersion()
                + ", execucao " + resp.executionId() + ", corte UTC " + resp.asOf()
                + "). Produto experimental de apoio analitico; nao utilizar para preservacao de vida ou propriedade.";

        return new ProviderResult(
                OutcomeStatus.AVAILABLE,
                okReason,
                manifest.predictionTarget(),
                manifest.validatedHorizon(),
                manifest.modelVersion(),
                mergedMetrics,
                Collections.unmodifiableList(validatedPoints)
        );
    }

    private Map<String, Object> buildEvaluationMetadata(V1EvaluationSummary remoteEval) {
        Map<String, Object> result = new LinkedHashMap<>(manifest.outOfSampleMetrics());
        if (remoteEval != null && remoteEval.metrics() != null) {
            for (Map.Entry<String, Double> entry : remoteEval.metrics().entrySet()) {
                if (entry.getKey() == null || entry.getKey().isBlank()
                        || entry.getValue() == null || !Double.isFinite(entry.getValue())) {
                    throw PredictionProviderException.invalidResponse(
                            "Metrica de avaliacao invalida ou nao finita na resposta do provedor.",
                            "invalid evaluationSummary metric");
                }
            }
        }
        return Collections.unmodifiableMap(result);
    }

    public record V1PredictRequest(
            String protocolVersion,
            String requestId,
            V1BBox bbox,
            @JsonFormat(shape = JsonFormat.Shape.STRING) Instant asOf,
            V1Window requestedWindow,
            String predictionTarget,
            String featureContractId,
            String modelId,
            String modelVersion
    ) {}

    public record V1BBox(double minLon, double minLat, double maxLon, double maxLat) {}

    public record V1Window(
            @JsonFormat(shape = JsonFormat.Shape.STRING) LocalDate start,
            @JsonFormat(shape = JsonFormat.Shape.STRING) LocalDate end
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record V1PredictResponse(
            String protocolVersion,
            String executionId,
            String modelId,
            String modelVersion,
            String predictionTarget,
            Dto.PredictionResponse.ValidatedHorizon validatedHorizon,
            Instant asOf,
            V1Window predictedInterval,
            V1Coverage coverage,
            V1EvaluationSummary evaluationSummary,
            List<V1PredictionItem> predictions
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record V1Coverage(
            String status,
            Double observedCoverageRatio,
            String reason
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record V1EvaluationSummary(
            String datasetId,
            String temporalSplitPolicy,
            Map<String, Double> metrics
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record V1PredictionItem(
            Double lat,
            Double lon,
            Instant forecastTimestamp,
            Double occurrenceProbability,
            Double predictedFrpMw,
            String frpUnit,
            String confidenceLevel
    ) {}
}
