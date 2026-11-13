package com.fireatlas.sensor.entity;

/** Instrumento de observacao (MODIS ou VIIRS), do catalogo da tabela sensor. */
public record Sensor(String code, String name, int nominalResolutionMeters, String description) {

    public Sensor {
        if (code == null || code.isBlank()) throw new IllegalArgumentException("codigo do sensor obrigatorio");
        if (nominalResolutionMeters <= 0) throw new IllegalArgumentException("resolucao nominal deve ser positiva");
    }
}
