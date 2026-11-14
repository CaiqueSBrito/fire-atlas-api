package com.fireatlas.forecast.dto;

import java.time.LocalDate;

/** Uma data de emissao disponivel em /forecast/issues. */
public record IssueItem(LocalDate issueDate, long cells, long activeCells) {}
