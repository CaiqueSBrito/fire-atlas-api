package com.fireatlas.prediction.integration;

import com.fireatlas.lake.integration.AthenaQuery;
import com.fireatlas.lake.integration.LakeStorage;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * Liga o provedor que le a previsao do time de dados direto no data lake ({@code gold/forecast}, pelo Athena) quando
 * {@code fireatlas.prediction.source=forecast-table}. Por padrao NADA muda: o provedor desligado e o HTTP continuam
 * como definidos em {@link PredictionModelConfig}; este bean e {@code @Primary} so quando a propriedade esta ligada.
 */
@Configuration
@ConditionalOnProperty(prefix = "fireatlas.prediction", name = "source", havingValue = "forecast-table")
public class ForecastPredictionConfig {

    @Bean
    @Primary
    public PredictionProvider forecastTablePredictionProvider(
            AthenaQuery athena, LakeStorage storage,
            @Value("${fireatlas.prediction.max-predictions:5000}") int maxPredictions) {
        return new ForecastTablePredictionProvider(athena, storage, maxPredictions);
    }
}
