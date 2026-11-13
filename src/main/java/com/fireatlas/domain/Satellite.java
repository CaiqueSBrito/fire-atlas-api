package com.fireatlas.domain;

/** Plataforma que carrega um sensor. code e o valor da coluna satellite nos CSVs da FIRMS (Terra, Aqua, SNPP...). */
public record Satellite(String code, String sensor, String platformName, String sourceProduct) {

    public Satellite {
        if (code == null || code.isBlank()) throw new IllegalArgumentException("codigo do satelite obrigatorio");
        if (sensor == null || sensor.isBlank()) throw new IllegalArgumentException("sensor do satelite obrigatorio");
    }
}
