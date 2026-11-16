package com.fireatlas.gold.bootstrap.entity;

import com.fasterxml.jackson.annotation.JsonInclude;

/** Arquivos confirmados e operacoes de finalizacao sao contados separadamente; ready exige ambos. */
@JsonInclude(JsonInclude.Include.ALWAYS)
public record BootstrapStatus(String state, long completedUnits, long totalUnits, long rowsLoaded,
                              String message, String error, String phase,
                              long finalizationCompleted, long finalizationTotal) {}
