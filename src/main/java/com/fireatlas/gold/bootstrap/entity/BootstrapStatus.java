package com.fireatlas.gold.bootstrap.entity;

import com.fasterxml.jackson.annotation.JsonInclude;

/** Progresso real: somente arquivos integralmente confirmados contam como unidades concluidas. */
@JsonInclude(JsonInclude.Include.ALWAYS)
public record BootstrapStatus(String state, long completedUnits, long totalUnits, long rowsLoaded,
                              String message, String error) {}
