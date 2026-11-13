package com.fireatlas.query;

import java.util.Arrays;

/**
 * Sinalizacao de atividade incomum por z-score robusto (mediana/MAD, Iglewicz & Hoaglin).
 * z_mod = 0,6745 * (x - mediana) / MAD ; valores acima do limiar sao sinalizados.
 */
public final class Anomaly {

    public static final double THRESHOLD = 3.5;

    private Anomaly() {}

    public static double median(double[] values) {
        if (values.length == 0) return Double.NaN;
        double[] s = values.clone();
        Arrays.sort(s);
        int n = s.length;
        return n % 2 == 1 ? s[n / 2] : (s[n / 2 - 1] + s[n / 2]) / 2.0;
    }

    /** Retorna z robusto por valor, ou null se nao houver dispersao (MAD = 0) ou poucos dados. */
    public static Double[] robustZ(double[] values) {
        Double[] out = new Double[values.length];
        if (values.length < 7) return out;
        double med = median(values);
        double[] dev = new double[values.length];
        for (int i = 0; i < values.length; i++) dev[i] = Math.abs(values[i] - med);
        double mad = median(dev);
        if (mad == 0) return out;
        for (int i = 0; i < values.length; i++) out[i] = 0.6745 * (values[i] - med) / mad;
        return out;
    }
}
