package com.fireatlas.place.dto;

/** Cidade na resposta de /places/cities; use geonameId em city=. */
public record CityItem(long geonameId, String name, String countryCode, long cells, double nearestKm) {}
