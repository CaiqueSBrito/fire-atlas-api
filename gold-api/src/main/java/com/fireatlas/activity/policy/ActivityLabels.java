package com.fireatlas.activity.policy;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * Rotulo da unidade (obrigatorio na interface) e avisos de uso da atividade de fogo harmonizada, como o time de
 * dados definiu. O numero NAO e quantidade de focos nem de incendios.
 */
public final class ActivityLabels {

    public static final String UNIT_PT = "Atividade de fogo harmonizada (células ativas equivalentes ao MODIS Aqua)";
    public static final String UNIT_EN = "Harmonized fire activity (MODIS Aqua-equivalent active cells)";
    public static final String SOURCE = "gold/harmonized (time de dados): atividade harmonizada por celula H3 e dia "
            + "local, estimada para a escala do MODIS Aqua";
    public static final String NO_DATA = "Sem dados de atividade harmonizada para o recorte (contributing = 0 "
            + "significa sem dado, nao zero fogo).";

    /** A partir daqui a area e pequena demais: o erro mensal chega a cerca de 33% (~1.800 km2). */
    public static final double SMALL_AREA_KM2 = 2000.0;
    /** Meses mais recentes que isto sao dados NRT e podem mudar. */
    public static final int RECENT_DAYS = 90;

    private static final LocalDate TERRA_ONLY_FROM = LocalDate.of(2000, 11, 1);
    private static final LocalDate TERRA_ONLY_TO = LocalDate.of(2002, 6, 30);

    private ActivityLabels() {}

    /** Avisos sempre presentes mais os que dependem do periodo e da area consultados. */
    public static List<String> warnings(LocalDate start, LocalDate end, LocalDate today, double areaKm2) {
        List<String> w = new ArrayList<>();
        w.add("Unidade: celulas ativas equivalentes ao MODIS Aqua; nao e numero de focos nem de incendios.");
        w.add("O dia e o dia local de fogo (06:00 a 06:00 no horario solar local), nao UTC.");
        w.add("contributing = 0 significa sem dado, nao zero fogo.");
        if (!end.isBefore(today)) w.add("O dia corrente fica incompleto ate fechar.");
        if (!end.isBefore(today.minusDays(RECENT_DAYS))) {
            w.add("Meses recentes sao dados NRT (quase em tempo real) e podem mudar.");
        }
        if (!start.isAfter(TERRA_ONLY_TO) && !end.isBefore(TERRA_ONLY_FROM)) {
            w.add("De 2000-11 a 2002-06 so ha o satelite Terra: a estimativa e a mais fraca.");
        }
        if (areaKm2 > 0 && areaKm2 < SMALL_AREA_KM2) {
            w.add("Area pequena (~" + Math.round(areaKm2) + " km2): o erro mensal chega a cerca de 33%.");
        }
        return w;
    }
}
