package com.fireatlas.gold.sensor.policy;

import com.fireatlas.gold.common.policy.InvalidParameterException;
import java.util.List;
import java.util.Locale;

/** Selecao de sensores aceita pelas consultas: MODIS, VIIRS ou ambos. */
public final class Sensors {

    /** Resolucao nominal no nadir de cada sensor, em metros (a mesma do catalogo da tabela {@code sensor}). */
    private static final java.util.Map<String, Integer> NOMINAL_RESOLUTION_M =
            java.util.Map.of("MODIS", 1000, "VIIRS", 375);

    private Sensors() {}

    /** Resolucao nominal do sensor em metros; 0 quando o codigo e desconhecido. */
    public static int nominalResolutionM(String sensor) {
        return NOMINAL_RESOLUTION_M.getOrDefault(sensor, 0);
    }

    /** MODIS | VIIRS | BOTH (ou AMBOS); vazio significa ambos. */
    public static List<String> parse(String sensor) {
        String s = sensor == null || sensor.isBlank() ? "BOTH" : sensor.trim().toUpperCase(Locale.ROOT);
        return switch (s) {
            case "MODIS" -> List.of("MODIS");
            case "VIIRS" -> List.of("VIIRS");
            case "BOTH", "AMBOS" -> List.of("MODIS", "VIIRS");
            default -> throw new InvalidParameterException("sensor", "sensor deve ser MODIS, VIIRS ou BOTH");
        };
    }
}
