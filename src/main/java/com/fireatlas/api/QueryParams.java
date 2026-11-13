package com.fireatlas.api;

import com.fireatlas.domain.BBox;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Locale;

/** Parametros comuns validados: regiao, periodo e sensores. */
public record QueryParams(BBox bbox, LocalDate start, LocalDate end, List<String> sensors) {

    public static QueryParams of(String bbox, LocalDate start, LocalDate end, String sensor, int maxDays) {
        if (start == null || end == null) {
            throw new IllegalArgumentException("parametros start e end (YYYY-MM-DD) sao obrigatorios");
        }
        if (end.isBefore(start)) {
            throw new IllegalArgumentException("end deve ser igual ou posterior a start");
        }
        long days = ChronoUnit.DAYS.between(start, end) + 1;
        if (days > maxDays) {
            throw new IllegalArgumentException("periodo maximo para esta consulta: " + maxDays + " dias");
        }
        return new QueryParams(BBox.parse(bbox), start, end, parseSensors(sensor));
    }

    public static List<String> parseSensors(String sensor) {
        String s = sensor == null || sensor.isBlank() ? "BOTH" : sensor.trim().toUpperCase(Locale.ROOT);
        return switch (s) {
            case "MODIS" -> List.of("MODIS");
            case "VIIRS" -> List.of("VIIRS");
            case "BOTH", "AMBOS" -> List.of("MODIS", "VIIRS");
            default -> throw new IllegalArgumentException("sensor deve ser MODIS, VIIRS ou BOTH");
        };
    }
}
