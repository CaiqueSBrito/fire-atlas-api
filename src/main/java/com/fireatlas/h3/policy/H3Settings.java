package com.fireatlas.h3.policy;

import java.util.stream.IntStream;

/**
 * Resolucoes H3 em uso. A resolucao-base e a do indice H3 guardado em cada deteccao (a mais fina que a fonte
 * entrega); as resolucoes mais grossas (0 ate a base) sao ancestrais dela e alimentam as agregacoes.
 * Padrao 6: a camada ouro entrega de 0 a 6.
 */
public record H3Settings(int baseResolution) {

    public static final int DEFAULT_BASE_RESOLUTION = 6;

    public H3Settings {
        if (baseResolution < 0 || baseResolution > 15) {
            throw new IllegalArgumentException("resolucao-base H3 deve estar entre 0 e 15: " + baseResolution);
        }
    }

    public static H3Settings defaults() {
        return new H3Settings(DEFAULT_BASE_RESOLUTION);
    }

    /** Resolucoes pre-agregadas em daily_h3: de 0 ate a base. */
    public int[] aggResolutions() {
        return IntStream.rangeClosed(0, baseResolution).toArray();
    }

    /** Resolucao das celulas agregadas para um nivel de zoom do mapa . */
    public int resolutionForZoom(int zoom) {
        int res;
        if (zoom <= 3) res = 3;
        else if (zoom == 4) res = 4;
        else if (zoom == 5) res = 5;
        else res = 6;
        return Math.min(res, baseResolution);
    }
}
