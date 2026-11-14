package com.fireatlas.region.policy;

import java.text.Normalizer;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Continentes como a gold os escreve (ingles). Aceita tambem o nome em portugues, sem diferenciar maiusculas nem
 * acentos, e devolve sempre o nome da gold.
 */
public final class Continents {

    private static final Map<String, String> BY_KEY = new LinkedHashMap<>();

    static {
        add("Africa", "africa");
        add("Antarctica", "antarctica", "antartida");
        add("Asia", "asia");
        add("Europe", "europe", "europa");
        add("North America", "north america", "america do norte");
        add("Oceania", "oceania");
        add("South America", "south america", "america do sul");
        add("Seven seas (open ocean)", "seven seas (open ocean)", "seven seas", "oceano");
    }

    private Continents() {}

    private static void add(String name, String... keys) {
        for (String k : keys) BY_KEY.put(k, name);
    }

    /** Nome do continente na gold, ou vazio se nao for um continente conhecido. */
    public static Optional<String> resolve(String input) {
        if (input == null || input.isBlank()) return Optional.empty();
        String key = Normalizer.normalize(input.trim(), Normalizer.Form.NFD).replaceAll("\\p{M}", "")
                .toLowerCase(Locale.ROOT).replaceAll("\\s+", " ");
        return Optional.ofNullable(BY_KEY.get(key));
    }

    public static String validNames() {
        return String.join(", ", new java.util.LinkedHashSet<>(BY_KEY.values()));
    }
}
