package com.fireatlas.region.repository;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.time.LocalDate;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/** Filtros SQL compartilhados pelos repositorios: periodo, sensores e literal de array de celulas H3. */
public final class SqlFilters {

    private SqlFilters() {}

    /** Parametros de periodo e sensores (start, end, sensors). */
    public static MapSqlParameterSource period(LocalDate start, LocalDate end, List<String> sensors) {
        return new MapSqlParameterSource().addValue("start", start).addValue("end", end).addValue("sensors", sensors);
    }

    /** Literal de array do Postgres para bind como texto com CAST (evita milhares de placeholders). */
    public static String array(Collection<Long> values) {
        return values.stream().map(String::valueOf).collect(Collectors.joining(",", "{", "}"));
    }
}
