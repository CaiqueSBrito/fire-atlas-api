package com.fireatlas.api;

import com.fireatlas.domain.Region;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Locale;

/** Parametros comuns validados: regiao (bbox ou celulas H3), periodo e sensores. */
public record QueryParams(Region region, LocalDate start, LocalDate end, List<String> sensors) {

    public static QueryParams of(String bbox, String cells, LocalDate start, LocalDate end, String sensor,
                                 int maxDays) {
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
        return new QueryParams(Region.of(bbox, cells), start, end, parseSensors(sensor));
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
