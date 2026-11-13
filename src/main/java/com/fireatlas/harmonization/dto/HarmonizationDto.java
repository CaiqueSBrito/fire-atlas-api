package com.fireatlas.harmonization.dto;

import com.fireatlas.harmonization.entity.HarmonizationResult;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

/** Contratos de resposta da harmonizacao MODIS x VIIRS. */
public final class HarmonizationDto {

    private HarmonizationDto() {}

    public record Params(int maxDistanceMeters, int maxMinutes, int modisSampleCap) {}

    public record Method(String name, String description, List<String> limitations) {}

    public record Response(String region, LocalDate start, LocalDate end, Params params,
                           HarmonizationResult result, Method method, String message) {}

    public record Side(String sensor, String satellite, double lat, double lon, Instant acquiredAt, Float frp,
                       String sourceFile) {}

    public record Match(Side modis, Side viirs, double distanceMeters, double minutesApart) {}

    public record MatchesResponse(String region, LocalDate start, LocalDate end, Params params, int count,
                                  boolean truncated, List<Match> matches, Method method, String message) {}
}
