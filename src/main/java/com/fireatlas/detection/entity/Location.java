package com.fireatlas.detection.entity;

import com.fireatlas.h3.policy.H3Index;

/**
 * Localizacao de uma deteccao: coordenadas WGS84 e, quando a fonte traz, o indice H3 (resolucao 8 ou mais fina).
 * O indice H3 vem pronto da fonte (data lake): o backend o consome e nao o calcula a partir de lat/lon.
 * h3 e nulo quando a fonte nao o fornece.
 */
public record Location(double lat, double lon, Long h3) {

    public Location {
        if (!(lat >= -90 && lat <= 90)) throw new IllegalArgumentException("latitude fora de -90..90: " + lat);
        if (!(lon >= -180 && lon <= 180)) throw new IllegalArgumentException("longitude fora de -180..180: " + lon);
    }

    /** Sem indice H3 (a fonte nao o trouxe). */
    public static Location of(double lat, double lon) {
        return new Location(lat, lon, null);
    }

    /** Indice H3 em hexadecimal (formato usado pela API), ou null se nao houver. */
    public String h3Hex() {
        return h3 == null ? null : H3Index.toHex(h3);
    }
}
