package com.fireatlas.activity.policy;

import java.util.Comparator;
import java.util.List;

/** Regra de anomalia mensal: mes atual contra a MEDIANA do mesmo mes nos anos anteriores (que tem dado). */
public final class MonthBaseline {

    /** Minimo de anos anteriores com dado para calcular a mediana e a anomalia (RN05). */
    public static final int MIN_YEARS = 3;

    private MonthBaseline() {}

    public static double median(List<Double> values) {
        List<Double> sorted = values.stream().sorted(Comparator.naturalOrder()).toList();
        int n = sorted.size();
        return n % 2 == 1 ? sorted.get(n / 2) : (sorted.get(n / 2 - 1) + sorted.get(n / 2)) / 2.0;
    }

    /** previous = somas do mes nos anos anteriores com dado; current = soma do mes consultado. */
    public static BaselineResult evaluate(List<Double> previous, double current) {
        if (previous.size() < MIN_YEARS) return new BaselineResult(false, null, null, null);
        double median = median(previous);
        Double ratio = median > 0 ? current / median : null;
        Double deviation = median > 0 ? (current - median) / median * 100.0 : null;
        return new BaselineResult(true, median, ratio, deviation);
    }
}
