package com.fireatlas.query;

import com.fireatlas.api.Dto;
import com.fireatlas.domain.BBox;
import com.fireatlas.prediction.DisabledPredictionProvider;
import com.fireatlas.prediction.PredictionProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.LocalDate;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Servico de aplicacao para consultas preditivas de focos de calor (NASA-020).
 *
 * <p>Orquestra a porta {@link PredictionProvider} sem depender diretamente de URL,
 * cliente HTTP, banco de dados ou framework de machine learning. Quando o provedor
 * esta desativado (padrao), preserva o retorno explicito {@code MODEL_NOT_VALIDATED}.
 */
@Service
public class PredictionService {

    public static final String STATUS_MODEL_NOT_VALIDATED =
            PredictionProvider.OutcomeStatus.MODEL_NOT_VALIDATED.name();
    public static final String REASON_MODEL_NOT_VALIDATED =
            DisabledPredictionProvider.REASON_MODEL_NOT_VALIDATED;

    private final PredictionProvider provider;
    private final Clock clock;

    public PredictionService() {
        this(new DisabledPredictionProvider(), Clock.systemUTC());
    }

    public PredictionService(PredictionProvider provider, Clock clock) {
        this.provider = Objects.requireNonNull(provider, "provider obrigatorio");
        this.clock = Objects.requireNonNull(clock, "clock obrigatorio");
    }

    @Autowired
    public PredictionService(Optional<PredictionProvider> provider, Optional<Clock> clock) {
        this(provider.orElseGet(DisabledPredictionProvider::new), clock.orElseGet(Clock::systemUTC));
    }

    public boolean isProviderEnabled() {
        return provider.isEnabled();
    }

    public Dto.PredictionResponse predict(BBox bbox, LocalDate start, LocalDate end) {
        Objects.requireNonNull(bbox, "bbox obrigatorio");
        Objects.requireNonNull(start, "start obrigatorio");
        Objects.requireNonNull(end, "end obrigatorio");
        if (end.isBefore(start)) {
            throw new IllegalArgumentException("end deve ser igual ou posterior a start");
        }

        PredictionProvider.ProviderRequest providerRequest = new PredictionProvider.ProviderRequest(
                UUID.randomUUID().toString(),
                bbox,
                clock.instant(),
                start,
                end
        );
        PredictionProvider.ProviderResult result = provider.predict(providerRequest);

        return new Dto.PredictionResponse(
                result.status().isAvailable(),
                result.status().name(),
                result.reason(),
                new Dto.PredictionResponse.RequestWindow(bbox.toString(), start, end),
                result.predictionTarget(),
                result.validatedHorizon(),
                result.modelVersion(),
                result.evaluationMetrics(),
                result.predictions()
        );
    }

    public Dto.PredictionResponse unavailableForWindow(BBox bbox, LocalDate start, LocalDate end) {
        return predict(bbox, start, end);
    }
}
