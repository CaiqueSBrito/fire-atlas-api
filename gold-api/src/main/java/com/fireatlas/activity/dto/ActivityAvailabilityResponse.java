package com.fireatlas.activity.dto;

import java.time.LocalDate;
import java.util.List;

/** Disponibilidade dos satelites no periodo (global: nao depende da regiao). */
public record ActivityAvailabilityResponse(LocalDate start, LocalDate end, int count, List<SatelliteDayItem> days,
                                           String source, String message) {}
