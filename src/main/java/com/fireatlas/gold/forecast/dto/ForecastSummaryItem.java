package com.fireatlas.gold.forecast.dto;

/**
 * Totais da regiao na emissao, independentes de minProbability, onlyActive e limit: celulas com previsao, celulas
 * ativas, maior e media da probabilidade (ausentes quando nao ha celulas).
 */
public record ForecastSummaryItem(long cells, long activeCells, Double maxProbability, Double meanProbability) {}
