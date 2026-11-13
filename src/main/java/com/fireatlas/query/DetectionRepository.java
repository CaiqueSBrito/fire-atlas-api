package com.fireatlas.query;

import com.fireatlas.api.Dto;
import com.fireatlas.domain.BBox;
import com.fireatlas.domain.H3Index;
import com.fireatlas.domain.Region;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Consultas ao PostGIS.
 * <ul>
 *   <li>Agregados (mapa afastado, calendario, serie temporal): celulas H3 em daily_h3.</li>
 *   <li>Deteccoes individuais: bbox usa o indice GiST (filtro exato); regiao por celulas usa o indice h3.</li>
 * </ul>
 */
@Repository
public class DetectionRepository {

    private final NamedParameterJdbcTemplate jdbc;

    public DetectionRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static MapSqlParameterSource base(LocalDate s, LocalDate e, List<String> sensors) {
        return new MapSqlParameterSource().addValue("start", s).addValue("end", e).addValue("sensors", sensors);
    }

    /** Filtro regional sobre "detection": bbox -> GiST (exato); celulas -> indice h3. */
    private static String regionSql(Region r, MapSqlParameterSource p) {
        if (r.isBBox()) {
            BBox b = r.bbox();
            p.addValue("minLon", b.minLon()).addValue("minLat", b.minLat())
                    .addValue("maxLon", b.maxLon()).addValue("maxLat", b.maxLat());
            return "geom && ST_MakeEnvelope(:minLon, :minLat, :maxLon, :maxLat, 4326)";
        }
        p.addValue("h3cells", arr(r.detectionCells()));
        return "h3 = ANY(CAST(:h3cells AS bigint[]))";
    }

    /** Literal de array do Postgres para bind como texto com CAST (evita milhares de placeholders). */
    static String arr(Collection<Long> values) {
        return values.stream().map(String::valueOf).collect(Collectors.joining(",", "{", "}"));
    }

    private static MapSqlParameterSource withCover(MapSqlParameterSource p, Region.Cover c) {
        return p.addValue("res", c.res()).addValue("cells", arr(c.cells()));
    }

    /** Pontos individuais (pede limit+1 para sinalizar truncamento). */
    public List<Dto.Point> points(Region region, LocalDate s, LocalDate e, List<String> sensors, int limit) {
        MapSqlParameterSource p = base(s, e, sensors).addValue("limit", limit + 1);
        String sql = """
                SELECT sensor, satellite, acq_ts, lat, lon, frp, confidence_class, daynight, brightness
                FROM detection
                WHERE acq_date BETWEEN :start AND :end
                  AND sensor IN (:sensors)
                  AND {region}
                ORDER BY acq_ts
                LIMIT :limit
                """.replace("{region}", regionSql(region, p));
        return jdbc.query(sql, p, (rs, i) -> new Dto.Point(
                rs.getString("sensor"), rs.getString("satellite"),
                rs.getTimestamp("acq_ts").toInstant(), rs.getDouble("lat"), rs.getDouble("lon"),
                getFloat(rs, "frp"), rs.getString("confidence_class"), rs.getString("daynight"),
                getFloat(rs, "brightness")));
    }

    /** Celulas H3 agregadas por sensor (daily_h3), rapidas mesmo para periodos longos. */
    public List<Dto.Cell> cells(Region.Cover cover, LocalDate s, LocalDate e, List<String> sensors, int limit) {
        String sql = """
                SELECT sensor, cell, sum(n) AS n, sum(frp_sum) AS frp_sum, max(frp_max) AS frp_max
                FROM daily_h3
                WHERE res = :res AND cell = ANY(CAST(:cells AS bigint[]))
                  AND acq_date BETWEEN :start AND :end
                  AND sensor IN (:sensors)
                GROUP BY sensor, cell
                LIMIT :limit
                """;
        MapSqlParameterSource p = withCover(base(s, e, sensors), cover).addValue("limit", limit + 1);
        return jdbc.query(sql, p, (rs, i) -> {
            long cell = rs.getLong("cell");
            double[] c = H3Index.center(cell);
            return new Dto.Cell(rs.getString("sensor"), H3Index.toHex(cell), cover.res(), c[0], c[1],
                    rs.getInt("n"), rs.getDouble("frp_sum"), getFloat(rs, "frp_max"));
        });
    }

    public record DayRow(LocalDate date, String sensor, int n, double frpSum) {}

    public List<DayRow> dailyTotals(Region.Cover cover, LocalDate s, LocalDate e, List<String> sensors) {
        String sql = """
                SELECT acq_date, sensor, sum(n) AS n, sum(frp_sum) AS frp_sum
                FROM daily_h3
                WHERE res = :res AND cell = ANY(CAST(:cells AS bigint[]))
                  AND acq_date BETWEEN :start AND :end
                  AND sensor IN (:sensors)
                GROUP BY acq_date, sensor
                ORDER BY acq_date, sensor
                """;
        return jdbc.query(sql, withCover(base(s, e, sensors), cover), (rs, i) -> new DayRow(
                rs.getObject("acq_date", LocalDate.class), rs.getString("sensor"),
                rs.getInt("n"), rs.getDouble("frp_sum")));
    }

    /** Estatisticas exatas por sensor (a partir das deteccoes). */
    public List<Dto.SensorStats> sensorStats(Region region, LocalDate s, LocalDate e, List<String> sensors) {
        MapSqlParameterSource p = base(s, e, sensors);
        String sql = """
                SELECT sensor,
                       count(*) AS n,
                       count(DISTINCT acq_date) AS days,
                       avg(frp) AS frp_mean,
                       percentile_cont(0.5) WITHIN GROUP (ORDER BY frp) AS frp_p50,
                       max(frp) AS frp_max,
                       coalesce(sum(frp), 0) AS frp_total,
                       count(*) FILTER (WHERE confidence_class = 'l') AS conf_low,
                       count(*) FILTER (WHERE confidence_class = 'n') AS conf_nominal,
                       count(*) FILTER (WHERE confidence_class = 'h') AS conf_high,
                       max(s.nominal_resolution_m) AS resolution_m
                FROM detection
                JOIN sensor s ON s.code = detection.sensor
                WHERE acq_date BETWEEN :start AND :end
                  AND sensor IN (:sensors)
                  AND {region}
                GROUP BY sensor
                ORDER BY sensor
                """.replace("{region}", regionSql(region, p));
        return jdbc.query(sql, p, (rs, i) -> {
            Map<String, Long> conf = new LinkedHashMap<>();
            conf.put("low", rs.getLong("conf_low"));
            conf.put("nominal", rs.getLong("conf_nominal"));
            conf.put("high", rs.getLong("conf_high"));
            String sensor = rs.getString("sensor");
            return new Dto.SensorStats(sensor, rs.getLong("n"), rs.getInt("days"),
                    getDouble(rs, "frp_mean"), getDouble(rs, "frp_p50"), getFloat(rs, "frp_max"),
                    rs.getDouble("frp_total"), conf, rs.getInt("resolution_m"));
        });
    }

    /**
     * Pareamento MODIS -> VIIRS por proximidade espaco-temporal (metodo v0, ver docs).
     * A distancia usa PostGIS (geografia): mais precisa que celulas H3 para um limiar em metros.
     */
    public Dto.HarmonizationResponse.Result harmonize(Region region, LocalDate s, LocalDate e,
                                                      int maxDistM, int maxMinutes, int modisCap) {
        double cosLat = Math.cos(Math.toRadians(Math.min(region.maxAbsLat(), 80)));
        double degTol = (maxDistM / (111_320.0 * cosLat)) * 1.2;
        MapSqlParameterSource p = base(s, e, List.of("MODIS", "VIIRS"))
                .addValue("cap", modisCap).addValue("degTol", degTol)
                .addValue("sec", maxMinutes * 60.0).addValue("dist", (double) maxDistM);
        String sql = """
                WITH m AS (
                    SELECT geom, acq_date, acq_ts, frp
                    FROM detection
                    WHERE sensor = 'MODIS' AND frp IS NOT NULL
                      AND acq_date BETWEEN :start AND :end
                      AND {region}
                    ORDER BY acq_ts
                    LIMIT :cap
                ), x AS (
                    SELECT m.frp AS mfrp, l.n, l.vfrp
                    FROM m CROSS JOIN LATERAL (
                        SELECT count(*) AS n, coalesce(sum(v.frp), 0) AS vfrp
                        FROM detection v
                        WHERE v.sensor = 'VIIRS'
                          AND v.acq_date BETWEEN m.acq_date - 1 AND m.acq_date + 1
                          AND v.geom && ST_Expand(m.geom, :degTol)
                          AND abs(extract(epoch FROM (v.acq_ts - m.acq_ts))) <= :sec
                          AND ST_Distance(v.geom::geography, m.geom::geography) <= :dist
                    ) l
                )
                SELECT count(*) AS total,
                       count(*) FILTER (WHERE n > 0) AS matched,
                       avg(mfrp) FILTER (WHERE n > 0) AS m_mean,
                       avg(vfrp) FILTER (WHERE n > 0) AS v_mean,
                       sum(vfrp) FILTER (WHERE n > 0) / nullif(sum(mfrp) FILTER (WHERE n > 0), 0) AS ratio,
                       corr(mfrp, vfrp) FILTER (WHERE n > 0) AS r,
                       percentile_cont(0.5) WITHIN GROUP (ORDER BY n) FILTER (WHERE n > 0) AS med_n
                FROM x
                """.replace("{region}", regionSql(region, p));
        return jdbc.queryForObject(sql, p, (rs, i) -> {
            long total = rs.getLong("total");
            long matched = rs.getLong("matched");
            return new Dto.HarmonizationResponse.Result(total, matched,
                    total == 0 ? null : matched / (double) total,
                    getDouble(rs, "m_mean"), getDouble(rs, "v_mean"), getDouble(rs, "ratio"),
                    getDouble(rs, "r"), getDouble(rs, "med_n"), total >= modisCap);
        });
    }

    public List<Dto.SourceInfo> sources() {
        return jdbc.getJdbcTemplate().query("""
                SELECT product, sensor, source_key, bbox, period_start, period_end, rows_loaded,
                       min_date, max_date, loaded_at
                FROM ingestion_log ORDER BY loaded_at DESC LIMIT 500
                """, (rs, i) -> new Dto.SourceInfo(rs.getString("product"), rs.getString("sensor"),
                rs.getString("source_key"), rs.getString("bbox"),
                rs.getObject("period_start", LocalDate.class), rs.getObject("period_end", LocalDate.class),
                rs.getLong("rows_loaded"), rs.getObject("min_date", LocalDate.class),
                rs.getObject("max_date", LocalDate.class), rs.getTimestamp("loaded_at").toInstant()));
    }

    public List<Dto.Coverage> coverage() {
        return jdbc.getJdbcTemplate().query("""
                SELECT sensor, min(acq_date) AS min_date, max(acq_date) AS max_date, sum(n) AS n
                FROM daily_h3 WHERE res = %d GROUP BY sensor ORDER BY sensor
                """.formatted(H3Index.BASE_RES), (rs, i) -> new Dto.Coverage(rs.getString("sensor"),
                rs.getObject("min_date", LocalDate.class), rs.getObject("max_date", LocalDate.class),
                rs.getLong("n")));
    }

    private static Float getFloat(ResultSet rs, String col) throws SQLException {
        float v = rs.getFloat(col);
        return rs.wasNull() ? null : v;
    }

    private static Double getDouble(ResultSet rs, String col) throws SQLException {
        double v = rs.getDouble(col);
        return rs.wasNull() ? null : v;
    }

    public static <T> List<T> trim(List<T> l, int max) {
        return l.size() > max ? new ArrayList<>(l.subList(0, max)) : l;
    }
}
