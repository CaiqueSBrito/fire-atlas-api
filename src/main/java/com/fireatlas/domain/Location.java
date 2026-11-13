package com.fireatlas.domain;

/** Localizacao de uma deteccao: coordenadas WGS84 e o indice H3 na resolucao base (8). */
public record Location(double lat, double lon, long h3) {

    public Location {
        if (!(lat >= -90 && lat <= 90)) throw new IllegalArgumentException("latitude fora de -90..90: " + lat);
        if (!(lon >= -180 && lon <= 180)) throw new IllegalArgumentException("longitude fora de -180..180: " + lon);
    }

    public static Location of(double lat, double lon) {
        return new Location(lat, lon, H3Index.cell(lat, lon));
    }

    /** Indice H3 em hexadecimal (formato usado pela API). */
    public String h3Hex() {
        return H3Index.toHex(h3);
    }
}
