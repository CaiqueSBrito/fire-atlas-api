package com.fireatlas.gold.activity.dto;

/** Parte do harmonized do dia numa faixa de temperatura de brilho (K); limite nulo = faixa aberta. */
public record TemperatureBin(String key, Double fromK, Double toK, double harmonized) {}
