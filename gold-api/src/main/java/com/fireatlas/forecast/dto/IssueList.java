package com.fireatlas.forecast.dto;

import java.util.List;

/** Datas de emissao da previsao disponiveis (mais recente primeiro). */
public record IssueList(int count, List<IssueItem> issues, String source, String message) {}
