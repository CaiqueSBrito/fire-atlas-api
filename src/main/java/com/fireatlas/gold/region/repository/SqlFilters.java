package com.fireatlas.gold.region.repository;

import com.fireatlas.gold.h3.policy.H3Index;
import com.fireatlas.gold.region.entity.Region;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.time.LocalDate;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/** Filtros SQL compartilhados pelos repositorios: periodo, sensores e regiao (celulas H3 ou lugar). */
public final class SqlFilters {

    private SqlFilters() {}

    /** Parametros de periodo e sensores (start, end, sensors). */
    public static MapSqlParameterSource period(LocalDate start, LocalDate end, List<String> sensors) {
        return new MapSqlParameterSource().addValue("start", start).addValue("end", end).addValue("sensors", sensors);
    }

    /** Parametros da regiao: resolucao e as celulas da lista, ou os dados do lugar (pais, continente, cidade). */
    public static MapSqlParameterSource regionParams(MapSqlParameterSource p, Region.Cover c) {
        p.addValue("res", c.res());
        Region.Place place = c.place();
        if (place == null) return p.addValue("cells", array(c.cells()));
        switch (place.kind()) {
            case COUNTRY, CONTINENT -> p.addValue("placeValue", place.value());
            case CITY -> p.addValue("placeCity", Long.parseLong(place.value()))
                    .addValue("placeRadius", place.cityRadiusKm());
        }
        return p;
    }

    /**
     * Condicao SQL sobre (resCol, cellCol) para as celulas da regiao: lista de celulas, ou a juncao com h3_place
     * (lugar), na mesma resolucao. Os parametros vem de {@link #regionParams}.
     */
    public static String regionSql(Region.Cover c, String resCol, String cellCol) {
        if (c.place() == null) return resCol + " = :res AND " + cellCol + " = ANY(CAST(:cells AS bigint[]))";
        return resCol + " = :res AND " + cellCol + " IN (SELECT pl.cell FROM h3_place pl WHERE pl.res = :res AND "
                + placeCondition(c.place()) + ")";
    }

    /**
     * Igual a {@link #regionSql}, para uma coluna de celula com resolucao fixa (ex.: forecast.h3_r6). Se a cobertura
     * for mais grossa que a coluna, compara o ancestral da celula na resolucao da cobertura (sem expandir filhas).
     * A lista vira semi-join com hash: com {@code = ANY} o Postgres compara cada linha com a lista inteira.
     */
    public static String regionSqlFixedRes(Region.Cover c, String cellCol, int cellRes) {
        String cell = c.res() < cellRes ? H3Index.parentSql(cellCol, c.res()) : cellCol;
        if (c.place() == null) return cell + " IN (SELECT unnest(CAST(:cells AS bigint[])))";
        return cell + " IN (SELECT pl.cell FROM h3_place pl WHERE pl.res = :res AND "
                + placeCondition(c.place()) + ")";
    }

    /** Condicao sobre h3_place (alias pl) para o lugar; os parametros vem de {@link #regionParams}. */
    public static String placeCondition(Region.Place place) {
        return switch (place.kind()) {
            case COUNTRY -> "(pl.country_iso_a2 = :placeValue OR pl.country_code = :placeValue)";
            case CONTINENT -> "pl.continent = :placeValue";
            case CITY -> "pl.city_geonameid = :placeCity AND pl.city_distance_km <= :placeRadius";
        };
    }

    /** Literal de array do Postgres para bind como texto com CAST (evita milhares de placeholders). */
    public static String array(Collection<Long> values) {
        return values.stream().map(String::valueOf).collect(Collectors.joining(",", "{", "}"));
    }
}
