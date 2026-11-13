package com.fireatlas.region.repository;

import com.fireatlas.region.entity.BBox;
import com.fireatlas.region.entity.Region;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.time.LocalDate;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/** Filtros SQL compartilhados pelos repositorios: periodo, sensores e regiao (bbox ou celulas H3). */
public final class SqlFilters {

    private SqlFilters() {}

    /** Parametros de periodo e sensores (start, end, sensors). */
    public static MapSqlParameterSource period(LocalDate start, LocalDate end, List<String> sensors) {
        return new MapSqlParameterSource().addValue("start", start).addValue("end", end).addValue("sensors", sensors);
    }

    /** Filtro regional sobre "detection": bbox -> GiST (exato); celulas -> indice h3 que veio da fonte. */
    public static String region(Region r, MapSqlParameterSource p, int baseResolution) {
        if (r.isBBox()) {
            BBox b = r.bbox();
            p.addValue("minLon", b.minLon()).addValue("minLat", b.minLat())
                    .addValue("maxLon", b.maxLon()).addValue("maxLat", b.maxLat());
            return "geom && ST_MakeEnvelope(:minLon, :minLat, :maxLon, :maxLat, 4326)";
        }
        p.addValue("h3cells", array(r.detectionCells(baseResolution)));
        return "h3 = ANY(CAST(:h3cells AS bigint[]))";
    }

    /** Literal de array do Postgres para bind como texto com CAST (evita milhares de placeholders). */
    public static String array(Collection<Long> values) {
        return values.stream().map(String::valueOf).collect(Collectors.joining(",", "{", "}"));
    }
}
