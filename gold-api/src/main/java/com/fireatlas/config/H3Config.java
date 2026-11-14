package com.fireatlas.config;

import com.fireatlas.h3.policy.H3Settings;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Resolucao-base do H3 (a do indice guardado em cada deteccao). */
@Configuration
public class H3Config {

    @Bean
    public H3Settings h3Settings(@Value("${fireatlas.h3.base-resolution}") int baseResolution) {
        return new H3Settings(baseResolution);
    }
}
