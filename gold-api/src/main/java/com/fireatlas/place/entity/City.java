package com.fireatlas.place.entity;

/**
 * Cidade (GeoNames cities1000) como cidade MAIS PROXIMA de celulas H3: cells = quantas celulas (resolucao 5) tem esta
 * cidade como a mais proxima dentro do raio pedido; nearestKm = a menor distancia entre elas.
 */
public record City(long geonameId, String name, String countryCode, long cells, double nearestKm) {}
