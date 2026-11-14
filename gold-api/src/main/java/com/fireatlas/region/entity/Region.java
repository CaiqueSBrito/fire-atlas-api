package com.fireatlas.region.entity;

import com.fireatlas.common.policy.InvalidParameterException;
import com.fireatlas.h3.policy.H3Index;
import com.fireatlas.region.policy.Continents;

import java.util.ArrayList;
import java.util.List;

/**
 * Regiao de consulta: celulas H3 (indices em hexadecimal) OU um lugar (pais, continente ou cidade). A gold so tem
 * agregados por celula H3, entao toda consulta exige celulas ou um lugar; o bbox e aceito na sintaxe mas rejeitado
 * com mensagem clara: a API nao converte bbox em celulas (o H3 e consumido, nao calculado).
 * <p>
 * Um lugar nao vira uma lista de celulas: a consulta junta a grade com {@code h3_place} (que o time de dados gerou)
 * na resolucao escolhida, sem limite de celulas.
 */
public final class Region {

    public static final int MAX_INPUT_CELLS = 2000;
    /** Maximo de celulas H3 na cobertura de uma consulta de mapa (limita o tamanho da consulta). */
    public static final int MAX_MAP_CELLS = 20_000;
    /** Maximo de celulas H3 na cobertura de calendario/serie temporal. */
    public static final int MAX_AGG_CELLS = 5_000;
    /** Resolucao usada por padrao nas consultas por pais ou continente. */
    public static final int PLACE_DEFAULT_RES = 4;
    /** A cidade so existe em h3_places nas resolucoes finas (5 e 6). */
    public static final int CITY_MIN_RES = 5;
    public static final int MAX_RES = 6;
    public static final double DEFAULT_CITY_RADIUS_KM = 25.0;

    public enum Kind { COUNTRY, CONTINENT, CITY }

    /** Lugar: pais (ISO a2 ou a3), continente (nome da gold) ou cidade (geonameid, com raio em km). */
    public record Place(Kind kind, String value, double cityRadiusKm) {}

    /** Celulas H3 numa resolucao, ou um lugar (sem lista de celulas) na resolucao indicada. */
    public record Cover(int res, List<Long> cells, Place place) {
        public Cover(int res, List<Long> cells) {
            this(res, cells, null);
        }
    }

    private final BBox bbox;
    private final List<Long> cells;
    private final Place place;

    private Region(BBox bbox, List<Long> cells, Place place) {
        this.bbox = bbox;
        this.cells = cells;
        this.place = place;
    }

    /** Exatamente um entre bbox ("minLon,minLat,maxLon,maxLat") e cells (indices H3 em hexa separados por virgula). */
    public static Region of(String bbox, String cells) {
        return of(bbox, cells, null, null, null, null);
    }

    /** Exatamente um entre bbox (rejeitado nas consultas), cells, country, continent e city. */
    public static Region of(String bbox, String cells, String country, String continent, String city,
                            Double cityRadiusKm) {
        boolean hasB = filled(bbox);
        boolean hasC = filled(cells);
        boolean hasCountry = filled(country);
        boolean hasContinent = filled(continent);
        boolean hasCity = filled(city);
        int given = (hasB ? 1 : 0) + (hasC ? 1 : 0) + (hasCountry ? 1 : 0) + (hasContinent ? 1 : 0)
                + (hasCity ? 1 : 0);
        if (given != 1) {
            throw new InvalidParameterException("bbox/cells", given == 0
                    ? "informe cells (indices H3 separados por virgula), country, continent ou city"
                    : "use somente um entre cells, country, continent e city");
        }
        if (hasB) return new Region(BBox.parse(bbox), null, null);
        if (hasCountry) {
            String code = country.trim().toUpperCase(java.util.Locale.ROOT);
            if (!code.matches("[A-Z]{2,3}")) {
                throw new InvalidParameterException("country",
                        "country deve ser o codigo ISO do pais com 2 ou 3 letras (ex.: BR ou BRA)");
            }
            return new Region(null, null, new Place(Kind.COUNTRY, code, 0));
        }
        if (hasContinent) {
            String name = Continents.resolve(continent).orElseThrow(() -> new InvalidParameterException("continent",
                    "continent desconhecido; use um entre: " + Continents.validNames()));
            return new Region(null, null, new Place(Kind.CONTINENT, name, 0));
        }
        if (hasCity) {
            if (!city.trim().matches("\\d{1,12}")) {
                throw new InvalidParameterException("city", "city deve ser o geonameid numerico da cidade "
                        + "(veja /api/v1/places/cities)");
            }
            double radius = cityRadiusKm == null ? DEFAULT_CITY_RADIUS_KM : cityRadiusKm;
            if (radius < 1 || radius > 500) {
                throw new InvalidParameterException("cityRadiusKm", "cityRadiusKm deve estar entre 1 e 500");
            }
            return new Region(null, null, new Place(Kind.CITY, city.trim(), radius));
        }

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
        return new Region(null, List.copyOf(list), null);
    }

    private static boolean filled(String s) {
        return s != null && !s.isBlank();
    }

    public boolean isBBox() {
        return bbox != null;
    }

    public boolean isPlace() {
        return place != null;
    }

    public BBox bbox() {
        return bbox;
    }

    public Place place() {
        return place;
    }

    public String describe() {
        if (isBBox()) return bbox.toString();
        if (isPlace()) {
            return switch (place.kind()) {
                case COUNTRY -> "pais:" + place.value();
                case CONTINENT -> "continente:" + place.value();
                case CITY -> "cidade:" + place.value() + " (ate " + place.cityRadiusKm() + " km)";
            };
        }
        return "h3:" + cells.size() + " celulas";
    }

    /**
     * Celulas da regiao na resolucao preferida; se passar de maxCells, desce de resolucao ate caber.
     * Exige celulas H3 ou um lugar: o backend nao converte bbox em celulas.
     */
    public Cover coverFitting(int preferredRes, int maxCells) {
        if (isPlace()) return new Cover(placeRes(preferredRes), List.of(), place);
        if (isBBox()) {
            throw new InvalidParameterException("bbox", "esta consulta agregada so aceita cells (indices H3), "
                    + "country, continent ou city: a API nao converte bbox em celulas");
        }
        for (int r = preferredRes; r >= 0; r--) {
            if (size(r) <= maxCells) return new Cover(r, H3Index.toResolution(cells, r, maxCells));
        }
        throw new InvalidParameterException("cells", "regiao grande demais para esta consulta; reduza a area");
    }

    /**
     * Cobertura na resolucao das proprias celulas informadas (a mais fina entre elas, limitada pela base): a gold
     * ja traz cada resolucao pronta, entao nao ha expansao em celulas filhas quando as celulas tem a mesma resolucao.
     * Para um lugar, usa a resolucao padrao dele (4 para pais e continente; 5 ou mais para cidade).
     */
    public Cover coverNative(int baseResolution, int maxCells) {
        if (isPlace()) return new Cover(placeRes(Math.min(baseResolution, PLACE_DEFAULT_RES)), List.of(), place);
        if (isBBox()) return coverFitting(baseResolution, maxCells); // rejeita bbox com a mensagem padrao
        int finest = cells.stream().mapToInt(H3Index::resolution).max().orElse(0);
        return coverFitting(Math.min(baseResolution, finest), maxCells);
    }

    /** A cidade so existe em h3_places a partir da resolucao 5; os demais lugares existem de 0 a 6. */
    private int placeRes(int preferred) {
        int res = Math.max(0, Math.min(preferred, MAX_RES));
        return place.kind() == Kind.CITY ? Math.max(res, CITY_MIN_RES) : res;
    }

    private long size(int res) {
        long total = 0;
        for (long c : cells) total += H3Index.resolution(c) <= res ? H3Index.childrenCount(c, res) : 1;
        return total;
    }
}
