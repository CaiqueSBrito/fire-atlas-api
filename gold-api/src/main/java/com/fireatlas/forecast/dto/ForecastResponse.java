package com.fireatlas.forecast.dto;

import java.time.LocalDate;
import java.util.List;

/**
 * Resposta de /forecast: a emissao usada, as celulas ordenadas da maior para a menor probabilidade, a fonte e a
 * nota de metodo. Sem previsao para o pedido: lista vazia e message, sem numeros inventados.
 */
public record ForecastResponse(LocalDate issueDate, LocalDate latestIssueDate, String region, int count,
                               boolean truncated, List<ForecastItem> items, String source, String method,
                               String message) {}
