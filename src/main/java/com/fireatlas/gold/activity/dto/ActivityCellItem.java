package com.fireatlas.gold.activity.dto;

/** Uma celula do mapa harmonizado (h3 em hexadecimal); harmonized e a soma do periodo na unidade de meta. */
public record ActivityCellItem(String h3, int resolution, double harmonized, int contributing, int daysWithData,
                               long modisDetections, long viirsDetections) {}
