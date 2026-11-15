package com.fireatlas.gold.place.repository;

import com.fireatlas.gold.bootstrap.repository.AnnualCacheQuery;
import com.fireatlas.gold.place.entity.City;
import com.fireatlas.gold.place.entity.Continent;
import com.fireatlas.gold.place.entity.Country;
import com.fireatlas.gold.region.repository.TrinoFilters;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Catalogo mundial de lugares carregado no mesmo cache duravel da grade de 2026.
 * Os filtros usam as mesmas colunas da tabela Delta; a carga e iniciada somente pelo bootstrap.
 */
@Repository("goldPlaceRepository")
public class PlaceRepository {

    static final String PLACES = "h3_places";

    /** Pais e continente existem de 0 a 6; usa 4 (a mesma resolucao padrao das consultas por pais). */
    private static final int COUNTRY_RES = 4;
    /** A cidade so existe nas resolucoes 5 e 6. */
    private static final int CITY_RES = 5;

    private final AnnualCacheQuery queries;

    public PlaceRepository(AnnualCacheQuery queries) {
        this.queries = queries;
    }

    public List<Continent> findContinents() {
        String sql = "SELECT continent, count(DISTINCT country_iso_a2) AS countries"
                + " FROM " + PLACES
                + " WHERE resolution = " + COUNTRY_RES + " AND continent IS NOT NULL"
                + " GROUP BY continent ORDER BY continent";
        return queries.places(sql).stream()
                .map(r -> new Continent(r.str("continent"), r.intOr("countries", 0)))
                .toList();
    }

    /** continent nulo = todos os continentes. */
    public List<Country> findCountries(String continent) {
        String sql = "SELECT country_iso_a2, max(country_code) AS iso3, max(country_name) AS name,"
                + " max(country_name_pt) AS name_pt, max(continent) AS continent, count(*) AS cells"
                + " FROM " + PLACES
                + " WHERE resolution = " + COUNTRY_RES + " AND country_iso_a2 IS NOT NULL"
                + (continent == null ? "" : " AND continent = " + TrinoFilters.text(continent))
                + " GROUP BY country_iso_a2"
                + " ORDER BY coalesce(max(country_name_pt), max(country_name))";
        return queries.places(sql).stream()
                .map(r -> new Country(r.str("country_iso_a2"), r.str("iso3"), r.str("name"), r.str("name_pt"),
                        r.str("continent"), r.lngOr("cells", 0)))
                .toList();
    }

    /**
     * Cidades que sao a mais proxima de alguma celula (resolucao 5) dentro do raio. Filtra por pais (ISO a2) e por
     * trecho do nome (sem diferenciar maiusculas); as com mais celulas primeiro.
     */
    public List<City> findCities(String countryCode, String nameLike, double radiusKm, int limit) {
        String name = nameLike == null ? null : "%" + nameLike.trim().toLowerCase() + "%";
        String sql = "SELECT city_geonameid, max(city_name) AS name, max(city_country_code) AS country,"
                + " count(*) AS cells, min(city_distance_km) AS nearest"
                + " FROM " + PLACES
                + " WHERE resolution = " + CITY_RES + " AND city_geonameid IS NOT NULL"
                + " AND city_distance_km <= " + radiusKm
                + (countryCode == null ? "" : " AND city_country_code = " + TrinoFilters.text(countryCode))
                + (name == null ? "" : " AND lower(city_name) LIKE " + TrinoFilters.text(name))
                + " GROUP BY city_geonameid"
                + " ORDER BY cells DESC, name"
                + " LIMIT " + limit;
        return queries.places(sql).stream()
                .map(r -> new City(r.lngOr("city_geonameid", 0), r.str("name"), r.str("country"),
                        r.lngOr("cells", 0), r.dblOr("nearest", 0)))
                .toList();
    }
}
