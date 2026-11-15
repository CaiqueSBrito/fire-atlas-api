package com.fireatlas.gold.activity.policy;

import java.util.List;

/**
 * Faixas de temperatura de brilho (K) da atividade harmonizada, na ordem do contrato. {@code column} e a coluna em
 * harmonized_cell; na gold a coluna e {@code harmonized_} + column.
 */
public final class TemperatureBins {

    public record Bin(String key, String column, Double fromK, Double toK) {
        public String lakeColumn() {
            return "harmonized_" + column;
        }
    }

    public static final List<Bin> ALL = List.of(
            new Bin("lt320", "t_lt320", null, 320.0),
            new Bin("320", "t320", 320.0, 330.0),
            new Bin("330", "t330", 330.0, 340.0),
            new Bin("340", "t340", 340.0, 350.0),
            new Bin("350", "t350", 350.0, 360.0),
            new Bin("ge360", "t_ge360", 360.0, null),
            new Bin("none", "t_none", null, null));

    private TemperatureBins() {}

    public static String columns() {
        return String.join(", ", ALL.stream().map(Bin::column).toList());
    }
}
