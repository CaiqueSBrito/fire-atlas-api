package com.fireatlas.ranking.dto;

import com.fireatlas.common.dto.IndicatorMeta;

import java.util.List;

/** Contratos de resposta dos rankings de celulas H3 e das regioes criticas. */
public final class RankingDto {

    private RankingDto() {}

    /** Uma celula do ranking; critical = contagem no percentil critico ou acima (so quando ha celulas suficientes). */
    public record Item(int rank, String h3, int resolution, long detections, double frpSum, Float frpMax,
                       long modisDetections, long viirsDetections, boolean critical) {}

    /**
     * criticalThreshold e o valor de contagem do percentil pedido entre TODAS as celulas da regiao;
     * e nulo (e nenhuma celula e marcada critica) quando ha poucas celulas para definir o limiar.
     */
    public record Response(String metric, int resolution, long totalCells, int criticalPercentile,
                           Double criticalThreshold, List<Item> items, IndicatorMeta meta, String message) {}
}
