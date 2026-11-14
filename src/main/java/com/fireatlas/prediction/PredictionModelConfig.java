package com.fireatlas.prediction;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

/**
 * Configuracao Spring da infraestrutura de integracao preditiva (NASA-020).
 *
 * <p>Mantem {@link DisabledPredictionProvider} ativo por padrao ({@code fireatlas.prediction.enabled=false}).
 * Caso a ativacao seja solicitada ({@code enabled=true}), valida o manifesto cientifico completo na
 * inicializacao e falha imediatamente se qualquer pre-requisito estiver ausente ou invalido.
 */
@Configuration
@EnableConfigurationProperties(PredictionModelProperties.class)
public class PredictionModelConfig {

    @Bean
    public Clock predictionClock() {
        return Clock.systemUTC();
    }

    @Bean
    public PredictionProvider predictionProvider(PredictionModelProperties properties, ObjectMapper objectMapper) {
        if (!properties.isEnabled()) {
            return new DisabledPredictionProvider();
        }
        PredictionModelProperties.ValidatedManifest manifest = properties.validateForActivation();
        return new HttpPredictionProvider(manifest, objectMapper);
    }
}
