package com.fireatlas.activity.policy;

/**
 * Area aproximada de uma regiao por celulas H3, pela area media de uma celula em cada resolucao (valores publicados
 * do H3, em km2). E so uma estimativa para o aviso de area pequena: nada e convertido em coordenadas.
 */
public final class AreaEstimate {

    /** Area media de uma celula H3 por resolucao, de 0 a 6. */
    private static final double[] AVG_KM2 = {4_357_449.4, 609_788.4, 86_801.8, 12_393.4, 1_770.3, 252.9, 36.1};

    private AreaEstimate() {}

    public static double avgCellKm2(int resolution) {
        return AVG_KM2[Math.max(0, Math.min(resolution, AVG_KM2.length - 1))];
    }

    public static double estimateKm2(int resolution, long cells) {
        return cells * avgCellKm2(resolution);
    }
}
