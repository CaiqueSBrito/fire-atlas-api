package com.fireatlas.prediction;

import com.fireatlas.api.Dto;
import com.fireatlas.domain.BBox;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Porta hexagonal de integracao com provedor de inferencia preditiva (NASA-020).
 *
 * <p>Isola {@code PredictionService} e a camada HTTP de detalhes de protocolo, URL,
 * cliente HTTP ou tecnologia de machine learning.
 */
public interface PredictionProvider {

    /** Indica se ha modelo aprovado e provedor remoto operacionalmente habilitado. */
    boolean isEnabled();

    /**
     * Consulta o provedor para a regiao, janela temporal e instante de referencia ({@code asOf}).
     *
     * @throws PredictionProviderException em falhas operacionais de rede/timeout ou violacao de contrato remoto
     */
    ProviderResult predict(ProviderRequest request);

    enum OutcomeStatus {
        MODEL_NOT_VALIDATED(false),
        OUT_OF_APPROVED_SCOPE(false),
        INSUFFICIENT_DATA_COVERAGE(false),
        AVAILABLE(true);

        private final boolean available;

        OutcomeStatus(boolean available) {
            this.available = available;
        }

        public boolean isAvailable() {
            return available;
        }
    }

    record ProviderRequest(
            String requestId,
            BBox bbox,
            Instant asOf,
            LocalDate start,
            LocalDate end
    ) {
        public ProviderRequest {
            if (requestId == null || requestId.isBlank()) {
                throw new IllegalArgumentException("requestId obrigatorio");
            }
            Objects.requireNonNull(bbox, "bbox obrigatorio");
            Objects.requireNonNull(asOf, "asOf obrigatorio");
            Objects.requireNonNull(start, "start obrigatorio");
            Objects.requireNonNull(end, "end obrigatorio");
            if (end.isBefore(start)) {
                throw new IllegalArgumentException("end deve ser igual ou posterior a start");
            }
        }
    }

    record ProviderResult(
            OutcomeStatus status,
            String reason,
            String predictionTarget,
            Dto.PredictionResponse.ValidatedHorizon validatedHorizon,
            String modelVersion,
            Map<String, Object> evaluationMetrics,
            List<Dto.PredictionResponse.PredictionPoint> predictions
    ) {
        public ProviderResult {
            Objects.requireNonNull(status, "status obrigatorio");
            if (reason == null || reason.isBlank()) {
                throw new IllegalArgumentException("reason obrigatorio");
            }
            if (!status.isAvailable() && predictions != null) {
                throw new IllegalArgumentException("predictions deve ser null quando indisponivel");
            }
            if (status.isAvailable() && (predictions == null || predictions.isEmpty())) {
                throw new IllegalArgumentException("predictions nao pode ser nulo ou vazio quando AVAILABLE");
            }
        }
    }
}
