package com.fireatlas.analysis.policy;

import java.util.List;

/** Regra de comparacao com o historico: media, desvio-padrao amostral e z-score das mesmas datas em anos anteriores. */
public final class Historical {

    /** Minimo de anos com registro para calcular media historica e anomalia (RN05). */
    public static final int MIN_YEARS = 3;
    /** |z| a partir do qual o periodo atual e considerado anomalo em relacao ao historico. */
    public static final double Z_THRESHOLD = 2.0;

    private Historical() {}

    public record Result(boolean sufficient, Double mean, Double stdDev, Double zScore, Boolean anomalous,
                         Double changeVsMeanPct) {}

    /** perDay = deteccoes/dia de cada ano anterior COM registro; current = deteccoes/dia do periodo atual. */
    public static Result evaluate(List<Double> perDay, double current) {
        if (perDay.size() < MIN_YEARS) return new Result(false, null, null, null, null, null);
        double mean = perDay.stream().mapToDouble(Double::doubleValue).average().orElse(0);
        double var = perDay.stream().mapToDouble(v -> (v - mean) * (v - mean)).sum() / (perDay.size() - 1);
        double sd = Math.sqrt(var);
        Double z = sd > 0 ? (current - mean) / sd : null;
        Double change = mean > 0 ? (current - mean) / mean * 100.0 : null;
        return new Result(true, mean, sd, z, z == null ? null : Math.abs(z) >= Z_THRESHOLD, change);
    }
}
