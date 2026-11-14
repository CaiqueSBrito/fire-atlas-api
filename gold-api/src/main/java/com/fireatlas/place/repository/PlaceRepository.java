package com.fireatlas.place.repository;

import com.fireatlas.place.entity.City;
import com.fireatlas.place.entity.Continent;
import com.fireatlas.place.entity.Country;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

/** Consultas ao catalogo de lugares (h3_place, vindo da gold/h3_places). */
@Repository
public class PlaceRepository {

    /** Pais e continente existem de 0 a 6; usa 4 (a mesma resolucao padrao das consultas por pais). */
    private static final int COUNTRY_RES = 4;
    /** A cidade so existe nas resolucoes 5 e 6. */
    private static final int CITY_RES = 5;

    private final NamedParameterJdbcTemplate jdbc;

    public PlaceRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public List<Continent> findContinents() {
        return jdbc.query("""
                SELECT continent, count(DISTINCT country_iso_a2) AS countries
                FROM h3_place
                WHERE res = :res AND continent IS NOT NULL
                GROUP BY continent
                ORDER BY continent
                """, new MapSqlParameterSource("res", COUNTRY_RES),
                (rs, i) -> new Continent(rs.getString("continent"), rs.getInt("countries")));
    }

    /** continent nulo = todos os continentes. */
    public List<Country> findCountries(String continent) {
        MapSqlParameterSource p = new MapSqlParameterSource("res", COUNTRY_RES).addValue("continent", continent);
        return jdbc.query("""
                SELECT country_iso_a2, max(country_code) AS iso3, max(country_name) AS name,
                       max(country_name_pt) AS name_pt, max(continent) AS continent, count(*) AS cells
                FROM h3_place
                WHERE res = :res AND country_iso_a2 IS NOT NULL
                  AND (CAST(:continent AS text) IS NULL OR continent = :continent)
                GROUP BY country_iso_a2
                ORDER BY coalesce(max(country_name_pt), max(country_name))
                """, p, (rs, i) -> new Country(rs.getString("country_iso_a2"), rs.getString("iso3"),
                rs.getString("name"), rs.getString("name_pt"), rs.getString("continent"), rs.getLong("cells")));
    }

    /**
     * Cidades que sao a mais proxima de alguma celula (resolucao 5) dentro do raio. Filtra por pais (ISO a2) e por
     * trecho do nome (sem diferenciar maiusculas); as com mais celulas primeiro.
     */
    public List<City> findCities(String countryCode, String nameLike, double radiusKm, int limit) {
        MapSqlParameterSource p = new MapSqlParameterSource("res", CITY_RES).addValue("radius", radiusKm)
                .addValue("country", countryCode)
                .addValue("name", nameLike == null ? null : "%" + nameLike.trim().toLowerCase() + "%")
                .addValue("limit", limit);
        return jdbc.query("""
                SELECT city_geonameid, max(city_name) AS name, max(city_country_code) AS country,
                       count(*) AS cells, min(city_distance_km) AS nearest
                FROM h3_place
                WHERE res = :res AND city_geonameid IS NOT NULL AND city_distance_km <= :radius
                  AND (CAST(:country AS text) IS NULL OR city_country_code = :country)
                  AND (CAST(:name AS text) IS NULL OR lower(city_name) LIKE :name)
                GROUP BY city_geonameid
                ORDER BY cells DESC, name
                LIMIT :limit
                """, p, (rs, i) -> new City(rs.getLong("city_geonameid"), rs.getString("name"),
                rs.getString("country"), rs.getLong("cells"), rs.getDouble("nearest")));
    }
}
