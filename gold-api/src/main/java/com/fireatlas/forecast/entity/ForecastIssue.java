package com.fireatlas.forecast.entity;

import java.time.LocalDate;

/** Uma data de emissao da previsao presente no banco, com quantas celulas e quantas estao ativas. */
public record ForecastIssue(LocalDate issueDate, long cells, long activeCells) {}
