package com.fireatlas.gold.forecast.entity;

/** Totais da previsao numa regiao e emissao: celulas, celulas ativas, maior e media da probabilidade (nulas sem celulas). */
public record ForecastSummary(long cells, long activeCells, Double maxProbability, Double meanProbability) {}
