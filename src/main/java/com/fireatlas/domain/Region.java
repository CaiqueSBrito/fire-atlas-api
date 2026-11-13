package com.fireatlas.domain;

import java.util.ArrayList;
import java.util.List;

/**
 * Regiao de consulta: um retangulo (bbox) OU um conjunto de celulas H3.
 * Consultas agregadas (daily_h3) trabalham sempre com celulas H3; consultas sobre deteccoes usam o filtro
 * espacial exato do PostGIS para bbox e o indice h3 para celulas.
 */
public final class Region {

    public static final int MAX_INPUT_CELLS = 2000;
    public static final int MAX_DETECTION_CELLS = 50_000;

    /** Celulas H3 numa resolucao. */
    public record Cover(int res, List<Long> cells) {}

    private final BBox bbox;
    private final List<Long> cells;

    private Region(BBox bbox, List<Long> cells) {
        this.bbox = bbox;
        this.cells = cells;
    }

    /** Exatamente um entre bbox ("minLon,minLat,maxLon,maxLat") e cells (indices H3 em hexa separados por virgula). */
    public static Region of(String bbox, String cells) {
        boolean hasB = bbox != null && !bbox.isBlank();
        boolean hasC = cells != null && !cells.isBlank();
        if (hasB == hasC) {
            throw new IllegalArgumentException(hasB
                    ? "use bbox OU cells, nao os dois"
                    : "informe bbox (minLon,minLat,maxLon,maxLat) ou cells (indices H3 separados por virgula)");
        }
        if (hasB) return new Region(BBox.parse(bbox), null);

        String[] parts = cells.split(",");
        if (parts.length > MAX_INPUT_CELLS) {
            throw new IllegalArgumentException("maximo de " + MAX_INPUT_CELLS + " celulas H3 em cells");
        }
        List<Long> list = new ArrayList<>(parts.length);
        for (String p : parts) {
            long c = H3Index.fromHex(p);
            if (H3Index.resolution(c) > H3Index.BASE_RES) {
                throw new IllegalArgumentException("resolucao H3 maxima aceita: " + H3Index.BASE_RES);
            }
            list.add(c);
        }
        return new Region(null, List.copyOf(list));
    }

    public boolean isBBox() {
        return bbox != null;
    }

    public BBox bbox() {
        return bbox;
    }

    public String describe() {
        return isBBox() ? bbox.toString() : "h3:" + cells.size() + " celulas";
    }

    public double maxAbsLat() {
        if (isBBox()) return bbox.maxAbsLat();
        double m = 0;
        for (long c : cells) m = Math.max(m, Math.abs(H3Index.center(c)[0]));
        return m;
    }

    /** Celulas na resolucao BASE_RES, para filtrar deteccoes pelo indice h3 (somente regiao por celulas). */
    public List<Long> detectionCells() {
        return H3Index.toResolution(cells, H3Index.BASE_RES, MAX_DETECTION_CELLS);
    }

    /** Cobertura na resolucao preferida; se passar de maxCells, desce de resolucao ate caber. */
    public Cover coverFitting(int preferredRes, int maxCells) {
        for (int r = Math.min(preferredRes, H3Index.BASE_RES); r >= H3Index.MIN_AGG_RES; r--) {
            if (size(r) <= maxCells) return new Cover(r, cellsAt(r, maxCells));
        }
        throw new IllegalArgumentException("regiao grande demais para esta consulta; reduza a area");
    }

    private long size(int res) {
        if (isBBox()) return H3Index.estimateCover(bbox, res);
        long total = 0;
        for (long c : cells) total += H3Index.resolution(c) <= res ? H3Index.childrenCount(c, res) : 1;
        return total;
    }

    private List<Long> cellsAt(int res, int maxCells) {
        return isBBox() ? H3Index.cover(bbox, res) : H3Index.toResolution(cells, res, maxCells);
    }
}
