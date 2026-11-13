package com.fireatlas.preference.entity;

import java.util.List;

/** Regiao favorita do usuario anonimo: bbox (minLon,minLat,maxLon,maxLat) e/ou celulas H3 em hexadecimal. */
public record UserPreference(String name, List<Double> bbox, List<String> cells, String sensor, Integer zoom,
                             List<Double> center) {}
