package com.fireatlas.activity.entity;

import java.time.LocalDate;

/** Disponibilidade de um satelite num dia, como o pipeline de dados a registrou. Campos ausentes ficam nulos. */
public record SatelliteDay(LocalDate day, String platform, Integer units, Double typicalUnits, Boolean available,
                           Boolean contributes) {}
