package com.fireatlas.detection.repository;

import com.fireatlas.detection.entity.CellDistribution;
import com.fireatlas.detection.entity.PeriodTotal;
import com.fireatlas.detection.entity.RankedCell;
import com.fireatlas.h3.policy.H3Settings;
import com.fireatlas.detection.entity.CellAggregate;
import com.fireatlas.detection.entity.Coverage;
import com.fireatlas.detection.entity.DayTotal;
import com.fireatlas.detection.entity.Detection;
import com.fireatlas.detection.entity.SensorStats;
import com.fireatlas.h3.policy.H3Index;
import com.fireatlas.region.entity.Region;
import com.fireatlas.region.repository.SqlFilters;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Consultas de deteccoes ao PostGIS (camada de repositorio): so devolve tipos de entidade.
 * <ul>
 *   <li>Agregados (mapa afastado, calendario, serie temporal): celulas H3 em daily_h3.</li>
 *   <li>Deteccoes individuais: bbox usa o indice GiST (filtro exato); regiao por celulas usa o indice h3.</li>
 * </ul>
 */
@Repository
public class DetectionRepository {

    private static final DetectionRowMapper DETECTION_ROW_MAPPER = new DetectionRowMapper();

    private final NamedParameterJdbcTemplate jdbc;
    private final H3Settings h3;

    public DetectionRepository(NamedParameterJdbcTemplate jdbc, H3Settings h3) {
        this.jdbc = jdbc;
        this.h3 = h3;
    }

    private static MapSqlParameterSource withCover(MapSqlParameterSource p, Region.Cover c) {
        return p.addValue("res", c.res()).addValue("cells", SqlFilters.array(c.cells()));
    }

    /** Deteccoes individuais (pede limit+1 para o chamador poder sinalizar truncamento). */
    public List<Detection> findDetections(Region region, LocalDate s, LocalDate e, List<String> sensors,
                                          int limit) {
        MapSqlParameterSource p = SqlFilters.period(s, e, sensors).addValue("limit", limit + 1);
        String sql = ("SELECT " + DetectionRowMapper.COLUMNS + """

                FROM detection
                WHERE acq_date BETWEEN :start AND :end
                  AND sensor IN (:sensors)
                  AND {region}
                ORDER BY acq_ts
                LIMIT :limit
                """).replace("{region}", SqlFilters.region(region, p, h3.baseResolution()));
        return jdbc.query(sql, p, DETECTION_ROW_MAPPER);
    }

    /** Celulas H3 agregadas por sensor (daily_h3), rapidas mesmo para periodos longos. */
    public List<CellAggregate> findCells(Region.Cover cover, LocalDate s, LocalDate e, List<String> sensors,
                                         int limit) {
        String sql = """
                SELECT sensor, cell, sum(n) AS n, sum(frp_sum) AS frp_sum, max(frp_max) AS frp_max
                FROM daily_h3
                WHERE res = :res AND cell = ANY(CAST(:cells AS bigint[]))
                  AND acq_date BETWEEN :start AND :end
                  AND sensor IN (:sensors)
                GROUP BY sensor, cell
                LIMIT :limit
                """;
        MapSqlParameterSource p = withCover(SqlFilters.period(s, e, sensors), cover).addValue("limit", limit + 1);
        return jdbc.query(sql, p, (rs, i) -> new CellAggregate(rs.getString("sensor"), rs.getLong("cell"),
                rs.getInt("n"), rs.getDouble("frp_sum"), getFloat(rs, "frp_max")));
    }

    public List<DayTotal> findDailyTotals(Region.Cover cover, LocalDate s, LocalDate e, List<String> sensors) {
        String sql = """
                SELECT acq_date, sensor, sum(n) AS n, sum(frp_sum) AS frp_sum
                FROM daily_h3
                WHERE res = :res AND cell = ANY(CAST(:cells AS bigint[]))
                  AND acq_date BETWEEN :start AND :end
                  AND sensor IN (:sensors)
                GROUP BY acq_date, sensor
                ORDER BY acq_date, sensor
                """;
        return jdbc.query(sql, withCover(SqlFilters.period(s, e, sensors), cover), (rs, i) -> new DayTotal(
                rs.getObject("acq_date", LocalDate.class), rs.getString("sensor"),
                rs.getInt("n"), rs.getDouble("frp_sum")));
    }

    /** Totais por sensor no periodo, somando as celulas H3 da regiao (base da comparacao entre periodos). */
    public List<PeriodTotal> findPeriodTotals(Region.Cover cover, LocalDate s, LocalDate e, List<String> sensors) {
        String sql = """
                SELECT sensor, sum(n) AS n, sum(frp_sum::double precision) AS frp_sum,
                       count(DISTINCT acq_date) AS days
                FROM daily_h3
                WHERE res = :res AND cell = ANY(CAST(:cells AS bigint[]))
                  AND acq_date BETWEEN :start AND :end
                  AND sensor IN (:sensors)
                GROUP BY sensor
                ORDER BY sensor
                """;
        return jdbc.query(sql, withCover(SqlFilters.period(s, e, sensors), cover), (rs, i) -> new PeriodTotal(
                rs.getString("sensor"), rs.getLong("n"), rs.getDouble("frp_sum"), rs.getInt("days")));
    }

    /** Contagem exata de deteccoes por sensor (a partir da tabela de deteccoes), com o filtro regional exato. */
    public List<PeriodTotal> findDetectionTotals(Region region, LocalDate s, LocalDate e, List<String> sensors) {
        MapSqlParameterSource p = SqlFilters.period(s, e, sensors);
        String sql = """
                SELECT sensor, count(*) AS n, coalesce(sum(frp::double precision), 0) AS frp_sum,
                       count(DISTINCT acq_date) AS days
                FROM detection
                WHERE acq_date BETWEEN :start AND :end
                  AND sensor IN (:sensors)
                  AND {region}
                GROUP BY sensor
                ORDER BY sensor
                """.replace("{region}", SqlFilters.region(region, p, h3.baseResolution()));
        return jdbc.query(sql, p, (rs, i) -> new PeriodTotal(rs.getString("sensor"), rs.getLong("n"),
                rs.getDouble("frp_sum"), rs.getInt("days")));
    }

    /** Celulas H3 mais ativas (todos os sensores selecionados somados), ordenadas por contagem ou por FRP. */
    public List<RankedCell> findRanking(Region.Cover cover, LocalDate s, LocalDate e, List<String> sensors,
                                        boolean byFrp, int limit) {
        String order = byFrp ? "sum(frp_sum::double precision)" : "sum(n)";
        String sql = """
                SELECT cell, sum(n) AS n, sum(frp_sum::double precision) AS frp_sum, max(frp_max) AS frp_max,
                       coalesce(sum(n) FILTER (WHERE sensor = 'MODIS'), 0) AS modis,
                       coalesce(sum(n) FILTER (WHERE sensor = 'VIIRS'), 0) AS viirs
                FROM daily_h3
                WHERE res = :res AND cell = ANY(CAST(:cells AS bigint[]))
                  AND acq_date BETWEEN :start AND :end
                  AND sensor IN (:sensors)
                GROUP BY cell
                ORDER BY %s DESC, cell
                LIMIT :limit
                """.formatted(order);
        MapSqlParameterSource p = withCover(SqlFilters.period(s, e, sensors), cover).addValue("limit", limit);
        return jdbc.query(sql, p, (rs, i) -> new RankedCell(rs.getLong("cell"), rs.getLong("n"),
                rs.getDouble("frp_sum"), getFloat(rs, "frp_max"), rs.getLong("modis"), rs.getLong("viirs")));
    }

    /** Quantas celulas tem registro e o valor da contagem no percentil pedido (0-1): define as regioes criticas. */
    public CellDistribution findCellDistribution(Region.Cover cover, LocalDate s, LocalDate e, List<String> sensors,
                                                 double percentile) {
        String sql = """
                SELECT count(*) AS cells, percentile_cont(:pct) WITHIN GROUP (ORDER BY n) AS pct_value
                FROM (SELECT cell, sum(n) AS n
                      FROM daily_h3
                      WHERE res = :res AND cell = ANY(CAST(:cells AS bigint[]))
                        AND acq_date BETWEEN :start AND :end
                        AND sensor IN (:sensors)
                      GROUP BY cell) t
                """;
        MapSqlParameterSource p = withCover(SqlFilters.period(s, e, sensors), cover).addValue("pct", percentile);
        return jdbc.queryForObject(sql, p, (rs, i) -> new CellDistribution(rs.getLong("cells"),
                getDouble(rs, "pct_value")));
    }

    /** Estatisticas exatas por sensor (a partir das deteccoes); a resolucao nominal vem do catalogo sensor. */
    public List<SensorStats> findSensorStats(Region region, LocalDate s, LocalDate e, List<String> sensors) {
        MapSqlParameterSource p = SqlFilters.period(s, e, sensors);
        String sql = """
                SELECT sensor,
                       count(*) AS n,
                       count(DISTINCT acq_date) AS days,
                       avg(frp) AS frp_mean,
                       percentile_cont(0.5) WITHIN GROUP (ORDER BY frp) AS frp_p50,
                       max(frp) AS frp_max,
                       coalesce(sum(frp::double precision), 0) AS frp_total,
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
                """.replace("{region}", SqlFilters.region(region, p, h3.baseResolution()));
        return jdbc.query(sql, p, (rs, i) -> {
            Map<String, Long> conf = new LinkedHashMap<>();
            conf.put("low", rs.getLong("conf_low"));
            conf.put("nominal", rs.getLong("conf_nominal"));
            conf.put("high", rs.getLong("conf_high"));
            return new SensorStats(rs.getString("sensor"), rs.getLong("n"), rs.getInt("days"),
                    getDouble(rs, "frp_mean"), getDouble(rs, "frp_p50"), getFloat(rs, "frp_max"),
                    rs.getDouble("frp_total"), conf, rs.getInt("resolution_m"));
        });
    }

    public List<Coverage> findCoverage() {
        return jdbc.getJdbcTemplate().query("""
                SELECT sensor, min(acq_date) AS min_date, max(acq_date) AS max_date, sum(n) AS n
                FROM daily_h3 WHERE res = %d GROUP BY sensor ORDER BY sensor
                """.formatted(h3.baseResolution()), (rs, i) -> new Coverage(rs.getString("sensor"),
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
}
