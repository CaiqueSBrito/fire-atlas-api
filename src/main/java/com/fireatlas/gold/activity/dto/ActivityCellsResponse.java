package com.fireatlas.gold.activity.dto;

import java.util.List;

/**
 * Mapa da atividade harmonizada: celulas H3 da regiao na resolucao do zoom, da maior para a menor atividade. Sem
 * celulas com dado: lista vazia e message (sem dado nao e zero fogo).
 */
public record ActivityCellsResponse(int resolution, int count, boolean truncated, double total,
                                    List<ActivityCellItem> cells, ActivityMeta meta, String message) {}
