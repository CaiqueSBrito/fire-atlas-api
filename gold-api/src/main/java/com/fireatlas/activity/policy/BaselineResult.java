package com.fireatlas.activity.policy;

/**
 * Comparacao do mes atual com a mediana do mesmo mes nos anos anteriores. Sem anos suficientes nada e calculado
 * (sufficient = false e os numeros ficam nulos): a API nao inventa anomalia.
 */
public record BaselineResult(boolean sufficient, Double median, Double ratioToMedian, Double deviationPct) {}
