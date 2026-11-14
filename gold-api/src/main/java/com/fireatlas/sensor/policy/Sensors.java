package com.fireatlas.sensor.policy;

import com.fireatlas.common.policy.InvalidParameterException;
import java.util.List;
import java.util.Locale;

/** Selecao de sensores aceita pelas consultas: MODIS, VIIRS ou ambos. */
public final class Sensors {

    private Sensors() {}

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
