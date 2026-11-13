package com.fireatlas.domain;

import com.uber.h3core.AreaUnit;
import com.uber.h3core.H3Core;
import com.uber.h3core.util.LatLng;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Utilitarios H3. Cada deteccao guarda o indice na resolucao 8 ({@link #BASE_RES}); resolucoes mais grossas
 * sao ancestrais desse indice e podem ser calculadas no proprio SQL por operacoes de bits.
 */
public final class H3Index {

    public static final int BASE_RES = 8;
    public static final int MIN_AGG_RES = 3;
    /** Resolucoes pre-agregadas em daily_h3. */
    public static final int[] AGG_RESOLUTIONS = {3, 4, 5, 6, 7, 8};
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

    public static long cell(double lat, double lon) {
        return H3.latLngToCell(lat, lon, BASE_RES);
    }

    public static long cell(double lat, double lon, int res) {
        return H3.latLngToCell(lat, lon, res);
    }

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

    public static int resolution(long cell) {
        return H3.getResolution(cell);
    }

    /** @return {lat, lon} do centro da celula */
    public static double[] center(long cell) {
        LatLng ll = H3.cellToLatLng(cell);
        return new double[]{ll.lat, ll.lng};
    }

    /** Resolucao das celulas agregadas para um nivel de zoom do mapa (abaixo de POINTS_MIN_ZOOM). */
    public static int resolutionForZoom(int zoom) {
        if (zoom <= 3) return 3;
        if (zoom == 4) return 4;
        if (zoom == 5) return 5;
        if (zoom <= 7) return 6;
        if (zoom == 8) return 7;
        return BASE_RES;
    }

    /** Estimativa do numero de celulas que cobrem o retangulo na resolucao dada. */
    public static long estimateCover(BBox b, int res) {
        double midLat = (b.minLat() + b.maxLat()) / 2;
        double heightKm = (b.maxLat() - b.minLat()) * 111.32;
        double widthKm = (b.maxLon() - b.minLon()) * 111.32 * Math.cos(Math.toRadians(midLat));
        double area = H3.getHexagonAreaAvg(res, AreaUnit.km2);
        return Math.max(1, (long) Math.ceil(heightKm * widthKm / area));
    }

    /** Celulas cujo centro cai dentro do retangulo; se nenhuma, a celula do centro do retangulo. */
    public static List<Long> cover(BBox b, int res) {
        List<LatLng> ring = List.of(
                new LatLng(b.minLat(), b.minLon()), new LatLng(b.minLat(), b.maxLon()),
                new LatLng(b.maxLat(), b.maxLon()), new LatLng(b.maxLat(), b.minLon()));
        List<Long> cells = H3.polygonToCells(ring, List.of(), res);
        if (cells.isEmpty()) {
            cells = List.of(H3.latLngToCell((b.minLat() + b.maxLat()) / 2, (b.minLon() + b.maxLon()) / 2, res));
        }
        return cells;
    }

    public static long childrenCount(long cell, int res) {
        return H3.cellToChildrenSize(cell, res);
    }

    /** Converte celulas para a resolucao res (filhas se mais grossas, ancestral se mais finas). */
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
