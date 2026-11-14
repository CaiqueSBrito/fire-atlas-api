package com.fireatlas.activity.dto;

import java.time.LocalDate;

/** Disponibilidade de um satelite (terra, aqua, snpp, noaa20 ou noaa21) num dia. */
public record SatelliteDayItem(LocalDate date, String platform, Integer units, Double typicalUnits,
                               Boolean available, Boolean contributes) {}
