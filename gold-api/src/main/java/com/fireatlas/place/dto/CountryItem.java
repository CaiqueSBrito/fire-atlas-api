package com.fireatlas.place.dto;

/** Pais na resposta de /places/countries; use isoA2 em country=. */
public record CountryItem(String isoA2, String isoA3, String name, String namePt, String continent, long cells) {}
