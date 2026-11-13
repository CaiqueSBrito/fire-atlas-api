package com.fireatlas.region.entity;

import com.fireatlas.common.policy.InvalidParameterException;
import com.fireatlas.h3.policy.H3Index;
import java.util.ArrayList;
import java.util.List;

/**
 * Regiao de consulta: um retangulo (bbox) OU um conjunto de celulas H3.
 * <ul>
 *   <li>Consultas agregadas (mapa afastado, calendario, serie temporal) usam daily_h3 e exigem celulas H3: o
 *       backend nao converte bbox em celulas.</li>
 *   <li>Consultas sobre deteccoes aceitam as duas: bbox usa o filtro espacial exato do PostGIS; celulas usam o
 *       indice h3 que veio da fonte.</li>
 * </ul>
 */
public final class Region {

    public static final int MAX_INPUT_CELLS = 2000;
    public static final int MAX_DETECTION_CELLS = 50_000;
    /** Maximo de celulas H3 na cobertura de uma consulta de mapa (limita o tamanho da consulta). */
    public static final int MAX_MAP_CELLS = 20_000;
    /** Maximo de celulas H3 na cobertura de calendario/serie temporal. */
    public static final int MAX_AGG_CELLS = 5_000;
    /** Latitude maxima assumida na tolerancia da harmonizacao quando a regiao e dada por celulas (conservador). */
    private static final double CELLS_MAX_ABS_LAT = 80.0;

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
            throw new InvalidParameterException("bbox/cells", hasB
                    ? "use bbox OU cells, nao os dois"
                    : "informe bbox (minLon,minLat,maxLon,maxLat) ou cells (indices H3 separados por virgula)");
        }
        if (hasB) return new Region(BBox.parse(bbox), null);

        String[] parts = cells.split(",");
        if (parts.length > MAX_INPUT_CELLS) {
            throw new InvalidParameterException("cells", "maximo de " + MAX_INPUT_CELLS + " celulas H3 em cells");
        }
        List<Long> list = new ArrayList<>(parts.length);
        for (String p : parts) {
            long c;
            try {
                c = H3Index.parseIndex(p);
            } catch (IllegalArgumentException e) {
                throw new InvalidParameterException("cells", e.getMessage());
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

    /** Latitude absoluta maxima da regiao; por celulas (sem converter H3 em coordenadas) assume um limite seguro. */
    public double maxAbsLat() {
        return isBBox() ? bbox.maxAbsLat() : CELLS_MAX_ABS_LAT;
    }

    /** Celulas na resolucao-base (a do indice h3 das deteccoes), para filtrar deteccoes (somente regiao por celulas). */
    public List<Long> detectionCells(int baseResolution) {
        return H3Index.toResolution(cells, baseResolution, MAX_DETECTION_CELLS);
    }

    /**
     * Celulas da regiao na resolucao preferida; se passar de maxCells, desce de resolucao ate caber.
     * Exige regiao por celulas H3: o backend nao converte bbox em celulas.
     */
    public Cover coverFitting(int preferredRes, int maxCells) {
        if (isBBox()) {
            throw new InvalidParameterException("bbox", "esta consulta agregada so aceita cells (indices H3): "
                    + "a API nao converte bbox em celulas");
        }
        for (int r = preferredRes; r >= 0; r--) {
            if (size(r) <= maxCells) return new Cover(r, H3Index.toResolution(cells, r, maxCells));
        }
        throw new InvalidParameterException("cells", "regiao grande demais para esta consulta; reduza a area");
    }

    private long size(int res) {
        long total = 0;
        for (long c : cells) total += H3Index.resolution(c) <= res ? H3Index.childrenCount(c, res) : 1;
        return total;
    }
}
