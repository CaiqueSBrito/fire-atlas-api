package com.fireatlas.query;

import com.fireatlas.api.Dto;
import com.fireatlas.domain.BBox;
import com.fireatlas.domain.GridMath;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Consultas ao PostGIS. Toda consulta usa bbox + intervalo de datas (poda de particoes + indice GiST). */
@Repository
public class DetectionRepository {

    private final NamedParameterJdbcTemplate jdbc;

    public DetectionRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static MapSqlParameterSource base(BBox b, LocalDate s, LocalDate e, List<String> sensors) {
        return new MapSqlParameterSource()
                .addValue("minLon", b.minLon()).addValue("minLat", b.minLat())
                .addValue("maxLon", b.maxLon()).addValue("maxLat", b.maxLat())
                .addValue("start", s).addValue("end", e)
                .addValue("sensors", sensors);
    }

    private static MapSqlParameterSource withCells(MapSqlParameterSource p, BBox b) {
        return p.addValue("cx0", GridMath.cx(b.minLon())).addValue("cx1", GridMath.cx(b.maxLon()))
                .addValue("cy0", GridMath.cy(b.minLat())).addValue("cy1", GridMath.cy(b.maxLat()));
    }

    /** Pontos individuais (pede limit+1 para sinalizar truncamento). */
    public List<Dto.Point> points(BBox b, LocalDate s, LocalDate e, List<String> sensors, int limit) {
        String sql = """
                SELECT sensor, satellite, acq_ts, lat, lon, frp, confidence_class, daynight, brightness
                FROM detection
                WHERE acq_date BETWEEN :start AND :end
                  AND sensor IN (:sensors)
                  AND geom && ST_MakeEnvelope(:minLon, :minLat, :maxLon, :maxLat, 4326)
                ORDER BY acq_ts
                LIMIT :limit
                """;
        return jdbc.query(sql, base(b, s, e, sensors).addValue("limit", limit + 1), (rs, i) -> new Dto.Point(
                rs.getString("sensor"), rs.getString("satellite"),
                rs.getTimestamp("acq_ts").toInstant(), rs.getDouble("lat"), rs.getDouble("lon"),
                getFloat(rs, "frp"), rs.getString("confidence_class"), rs.getString("daynight"),
                getFloat(rs, "brightness")));
    }

    /** Celulas agregadas a partir da grade diaria (rapido mesmo para periodos longos). */
    public List<Dto.Cell> cells(BBox b, LocalDate s, LocalDate e, List<String> sensors, int k, int limit) {
        String sql = """
                SELECT sensor, (cx / :k) AS gx, (cy / :k) AS gy,
                       sum(n) AS n, sum(frp_sum) AS frp_sum, max(frp_max) AS frp_max
                FROM daily_grid
                WHERE acq_date BETWEEN :start AND :end
                  AND sensor IN (:sensors)
                  AND cx BETWEEN :cx0 AND :cx1 AND cy BETWEEN :cy0 AND :cy1
                GROUP BY sensor, gx, gy
                LIMIT :limit
                """;
        MapSqlParameterSource p = withCells(base(b, s, e, sensors), b).addValue("k", k).addValue("limit", limit + 1);
        return jdbc.query(sql, p, (rs, i) -> new Dto.Cell(rs.getString("sensor"),
                GridMath.centerLat(rs.getInt("gy"), k), GridMath.centerLon(rs.getInt("gx"), k),
                rs.getInt("n"), rs.getDouble("frp_sum"), getFloat(rs, "frp_max")));
    }

    public record DayRow(LocalDate date, String sensor, int n, double frpSum) {}

    public List<DayRow> dailyTotals(BBox b, LocalDate s, LocalDate e, List<String> sensors) {
        String sql = """
                SELECT acq_date, sensor, sum(n) AS n, sum(frp_sum) AS frp_sum
                FROM daily_grid
                WHERE acq_date BETWEEN :start AND :end
                  AND sensor IN (:sensors)
                  AND cx BETWEEN :cx0 AND :cx1 AND cy BETWEEN :cy0 AND :cy1
                GROUP BY acq_date, sensor
                ORDER BY acq_date, sensor
                """;
        return jdbc.query(sql, withCells(base(b, s, e, sensors), b), (rs, i) -> new DayRow(
                rs.getObject("acq_date", LocalDate.class), rs.getString("sensor"),
                rs.getInt("n"), rs.getDouble("frp_sum")));
    }

    /** Estatisticas exatas por sensor (a partir das deteccoes). */
    public List<Dto.SensorStats> sensorStats(BBox b, LocalDate s, LocalDate e, List<String> sensors) {
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
                       count(*) FILTER (WHERE confidence_class = 'h') AS conf_high
                FROM detection
                WHERE acq_date BETWEEN :start AND :end
                  AND sensor IN (:sensors)
                  AND geom && ST_MakeEnvelope(:minLon, :minLat, :maxLon, :maxLat, 4326)
                GROUP BY sensor
                ORDER BY sensor
                """;
        return jdbc.query(sql, base(b, s, e, sensors), (rs, i) -> {
            Map<String, Long> conf = new LinkedHashMap<>();
            conf.put("low", rs.getLong("conf_low"));
            conf.put("nominal", rs.getLong("conf_nominal"));
            conf.put("high", rs.getLong("conf_high"));
            String sensor = rs.getString("sensor");
            return new Dto.SensorStats(sensor, rs.getLong("n"), rs.getInt("days"),
                    getDouble(rs, "frp_mean"), getDouble(rs, "frp_p50"), getFloat(rs, "frp_max"),
                    rs.getDouble("frp_total"), conf, "MODIS".equals(sensor) ? 1000 : 375);
        });
    }

    /** Pareamento MODIS -> VIIRS por proximidade espaco-temporal (metodo v0, ver docs). */
    public Dto.HarmonizationResponse.Result harmonize(BBox b, LocalDate s, LocalDate e,
                                                      int maxDistM, int maxMinutes, int modisCap) {
        double cosLat = Math.cos(Math.toRadians(Math.min(b.maxAbsLat(), 80)));
        double degTol = (maxDistM / (111_320.0 * cosLat)) * 1.2;
        String sql = """
                WITH m AS (
                    SELECT geom, acq_date, acq_ts, frp
                    FROM detection
                    WHERE sensor = 'MODIS' AND frp IS NOT NULL
                      AND acq_date BETWEEN :start AND :end
                      AND geom && ST_MakeEnvelope(:minLon, :minLat, :maxLon, :maxLat, 4326)
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
                """;
        MapSqlParameterSource p = base(b, s, e, List.of("MODIS", "VIIRS"))
                .addValue("cap", modisCap).addValue("degTol", degTol)
                .addValue("sec", maxMinutes * 60.0).addValue("dist", (double) maxDistM);
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
                FROM daily_grid GROUP BY sensor ORDER BY sensor
                """, (rs, i) -> new Dto.Coverage(rs.getString("sensor"),
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
