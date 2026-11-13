package com.fireatlas.h3.policy;

import com.uber.h3core.H3Core;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Utilitarios para CONSUMIR indices H3 que vem prontos da fonte (data lake): ler/validar, formatar e relacionar
 * celulas (ancestrais e filhas). Este backend NAO converte coordenadas em H3 nem H3 em coordenadas
 * (garantido por ArchitectureTest).
 */
public final class H3Index {

    /** A partir deste zoom a API devolve pontos individuais em vez de celulas. */
    public static final int POINTS_MIN_ZOOM = 9;

    private static final H3Core H3 = init();

    private H3Index() {}

    private static H3Core init() {
        try {
            return H3Core.newInstance();
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /** Ancestral (resolucao mais grossa) de uma celula: relacao hierarquica, nao conversao de coordenadas. */
    public static long parent(long cell, int res) {
        return H3.cellToParent(cell, res);
    }

    /** Ancestral por bits: mesma formula usada em SQL (parentSql). Digitos abaixo de res viram 7 e res e regravada. */
    public static long parentBits(long cell, int res) {
        long ones = (1L << (3 * (15 - res))) - 1;
        return ((cell | ones) & ~(15L << 52)) | ((long) res << 52);
    }

    /** Expressao SQL do ancestral na resolucao res para uma coluna bigint com indice H3 (res 8). */
    public static String parentSql(String column, int res) {
        return "(((%1$s | ((1::bigint << %2$d) - 1)) & ~(15::bigint << 52)) | (%3$d::bigint << 52))"
                .formatted(column, 3 * (15 - res), res);
    }

    public static String toHex(long cell) {
        return H3.h3ToString(cell);
    }

    public static long fromHex(String hex) {
        long c;
        try {
            c = H3.stringToH3(hex.trim());
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("indice H3 invalido: " + hex);
        }
        if (!H3.isValidCell(c)) throw new IllegalArgumentException("indice H3 invalido: " + hex);
        return c;
    }

    /**
     * Interpreta um indice H3 vindo da fonte: texto hexadecimal (ex.: 88a8b8c8d9fffff) ou inteiro de 64 bits em
     * decimal. Lanca IllegalArgumentException se nao for uma celula H3 valida.
     */
    public static long parseIndex(String raw) {
        String t = raw == null ? "" : raw.trim();
        // celulas H3 em decimal tem 18-19 digitos; em hexadecimal sempre trazem letras (f) nas resolucoes < 15
        boolean decimal = t.length() >= 18 && t.chars().allMatch(Character::isDigit);
        if (!decimal) return fromHex(t);
        long c;
        try {
            c = Long.parseLong(t);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("indice H3 invalido: " + raw);
        }
        if (!H3.isValidCell(c)) throw new IllegalArgumentException("indice H3 invalido: " + raw);
        return c;
    }

    public static int resolution(long cell) {
        return H3.getResolution(cell);
    }

    public static long childrenCount(long cell, int res) {
        return H3.cellToChildrenSize(cell, res);
    }

    /** Relaciona celulas na resolucao res (filhas se mais grossas, ancestral se mais finas). */
    public static List<Long> toResolution(Collection<Long> cells, int res, int cap) {
        Set<Long> out = new LinkedHashSet<>();
        for (long c : cells) {
            int r = resolution(c);
            if (r == res) {
                out.add(c);
            } else if (r > res) {
                out.add(parent(c, res));
            } else {
                if (childrenCount(c, res) + out.size() > cap) {
                    throw new IllegalArgumentException("regiao grande demais para esta consulta; reduza a area ou o zoom");
                }
                out.addAll(H3.cellToChildren(c, res));
            }
            if (out.size() > cap) {
                throw new IllegalArgumentException("regiao grande demais para esta consulta; reduza a area ou o zoom");
            }
        }
        return List.copyOf(out);
    }
}
