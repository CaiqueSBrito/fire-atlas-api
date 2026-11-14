package com.fireatlas.region.entity;

import com.fireatlas.common.policy.InvalidParameterException;
import com.fireatlas.h3.policy.H3Index;
import java.util.ArrayList;
import java.util.List;

/**
 * Regiao de consulta: celulas H3 (indices em hexadecimal). A gold so tem agregados por celula H3, entao toda
 * consulta exige celulas; o bbox e aceito na sintaxe mas rejeitado com mensagem clara: a API nao converte bbox em
 * celulas (o H3 e consumido, nao calculado).
 */
public final class Region {

    public static final int MAX_INPUT_CELLS = 2000;
    /** Maximo de celulas H3 na cobertura de uma consulta de mapa (limita o tamanho da consulta). */
    public static final int MAX_MAP_CELLS = 20_000;
    /** Maximo de celulas H3 na cobertura de calendario/serie temporal. */
    public static final int MAX_AGG_CELLS = 5_000;

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

    /**
     * Cobertura na resolucao das proprias celulas informadas (a mais fina entre elas, limitada pela base): a gold
     * ja traz cada resolucao pronta, entao nao ha expansao em celulas filhas quando as celulas tem a mesma resolucao.
     */
    public Cover coverNative(int baseResolution, int maxCells) {
        if (isBBox()) return coverFitting(baseResolution, maxCells); // rejeita bbox com a mensagem padrao
        int finest = cells.stream().mapToInt(H3Index::resolution).max().orElse(0);
        return coverFitting(Math.min(baseResolution, finest), maxCells);
    }

    private long size(int res) {
        long total = 0;
        for (long c : cells) total += H3Index.resolution(c) <= res ? H3Index.childrenCount(c, res) : 1;
        return total;
    }
}
