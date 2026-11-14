package com.fireatlas.prediction;

import java.util.Objects;

/**
 * Implementacao padrao desativada da porta {@link PredictionProvider}.
 *
 * <p>Garante que nenhuma chamada HTTP ou consulta de banco seja realizada enquanto nao
 * existir modelo preditivo cientificamente validado e aprovado.
 */
public class DisabledPredictionProvider implements PredictionProvider {

    public static final String REASON_MODEL_NOT_VALIDATED =
            "Nao existe modelo preditivo validado para o recorte solicitado. "
                    + "Os parametros start e end representam apenas a janela solicitada pelo cliente, "
                    + "nao um horizonte cientifico aprovado; anomalias historicas de /api/v1/timeseries "
                    + "nao constituem previsao.";

    @Override
    public boolean isEnabled() {
        return false;
    }

    @Override
    public ProviderResult predict(ProviderRequest request) {
        Objects.requireNonNull(request, "request obrigatorio");
        return new ProviderResult(
                OutcomeStatus.MODEL_NOT_VALIDATED,
                REASON_MODEL_NOT_VALIDATED,
                null,
                null,
                null,
                null,
                null
        );
    }
}
