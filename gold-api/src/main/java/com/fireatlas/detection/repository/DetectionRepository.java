package com.fireatlas.detection.repository;

import com.fireatlas.detection.entity.CellAggregate;
import com.fireatlas.detection.entity.CellDistribution;
import com.fireatlas.detection.entity.Coverage;
import com.fireatlas.detection.entity.DayTotal;
import com.fireatlas.detection.entity.PeriodTotal;
import com.fireatlas.detection.entity.RankedCell;
import com.fireatlas.detection.entity.SensorStats;
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
 * Consultas da grade H3 diaria (daily_h3, vinda da camada ouro) ao PostgreSQL: so devolve tipos de entidade.
 * Toda consulta recebe as celulas H3 da regiao numa resolucao (cobertura); a API nao converte coordenadas em H3.
 */
@Repository
public class DetectionRepository {

    private final NamedParameterJdbcTemplate jdbc;

    public DetectionRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static MapSqlParameterSource withCover(MapSqlParameterSource p, Region.Cover c) {
        return SqlFilters.regionParams(p, c);
    }

    /** Troca os marcadores de regiao pelo filtro (celulas ou lugar) da cobertura. */
    private static String fill(String sql, Region.Cover c) {
        return sql.replace("{cells}", SqlFilters.regionSql(c, "res", "cell"))
                .replace("{dcells}", SqlFilters.regionSql(c, "d.res", "d.cell"));
    }

    /** Celulas H3 agregadas por sensor (pede limit+1 para o chamador poder sinalizar truncamento). */
    public List<CellAggregate> findCells(Region.Cover cover, LocalDate s, LocalDate e, List<String> sensors,
                                         int limit) {
        String sql = """
                SELECT sensor, cell, sum(n) AS n, sum(frp_sum) AS frp_sum, max(frp_max) AS frp_max
                FROM daily_h3
                WHERE {cells}
                  AND acq_date BETWEEN :start AND :end
                  AND sensor IN (:sensors)
                GROUP BY sensor, cell
                LIMIT :limit
                """;
        MapSqlParameterSource p = withCover(SqlFilters.period(s, e, sensors), cover).addValue("limit", limit + 1);
        return jdbc.query(fill(sql, cover), p, (rs, i) -> new CellAggregate(rs.getString("sensor"), rs.getLong("cell"),
                rs.getInt("n"), rs.getDouble("frp_sum"), getFloat(rs, "frp_max")));
    }

    public List<DayTotal> findDailyTotals(Region.Cover cover, LocalDate s, LocalDate e, List<String> sensors) {
        String sql = """
                SELECT acq_date, sensor, sum(n) AS n, sum(frp_sum) AS frp_sum
                FROM daily_h3
                WHERE {cells}
                  AND acq_date BETWEEN :start AND :end
                  AND sensor IN (:sensors)
                GROUP BY acq_date, sensor
                ORDER BY acq_date, sensor
                """;
        return jdbc.query(fill(sql, cover), withCover(SqlFilters.period(s, e, sensors), cover), (rs, i) -> new DayTotal(
                rs.getObject("acq_date", LocalDate.class), rs.getString("sensor"),
                rs.getInt("n"), rs.getDouble("frp_sum")));
    }

    /** Totais por sensor no periodo, somando as celulas H3 da regiao (base da contagem e da comparacao). */
    public List<PeriodTotal> findPeriodTotals(Region.Cover cover, LocalDate s, LocalDate e, List<String> sensors) {
        String sql = """
                SELECT sensor, sum(n) AS n, coalesce(sum(frp_sum::double precision), 0) AS frp_sum,
                       count(DISTINCT acq_date) AS days
                FROM daily_h3
                WHERE {cells}
                  AND acq_date BETWEEN :start AND :end
                  AND sensor IN (:sensors)
                GROUP BY sensor
                ORDER BY sensor
                """;
        return jdbc.query(fill(sql, cover), withCover(SqlFilters.period(s, e, sensors), cover), (rs, i) -> new PeriodTotal(
                rs.getString("sensor"), rs.getLong("n"), rs.getDouble("frp_sum"), rs.getInt("days")));
    }

    /** Celulas H3 mais ativas (todos os sensores selecionados somados), ordenadas por contagem ou por FRP. */
    public List<RankedCell> findRanking(Region.Cover cover, LocalDate s, LocalDate e, List<String> sensors,
                                        boolean byFrp, int limit) {
        String order = byFrp ? "sum(frp_sum::double precision)" : "sum(n)";
        String sql = """
                SELECT cell, sum(n) AS n, coalesce(sum(frp_sum::double precision), 0) AS frp_sum,
                       max(frp_max) AS frp_max,
                       coalesce(sum(n) FILTER (WHERE sensor = 'MODIS'), 0) AS modis,
                       coalesce(sum(n) FILTER (WHERE sensor = 'VIIRS'), 0) AS viirs
                FROM daily_h3
                WHERE {cells}
                  AND acq_date BETWEEN :start AND :end
                  AND sensor IN (:sensors)
                GROUP BY cell
                ORDER BY %s DESC, cell
                LIMIT :limit
                """.formatted(order);
        MapSqlParameterSource p = withCover(SqlFilters.period(s, e, sensors), cover).addValue("limit", limit);
        return jdbc.query(fill(sql, cover), p, (rs, i) -> new RankedCell(rs.getLong("cell"), rs.getLong("n"),
                rs.getDouble("frp_sum"), getFloat(rs, "frp_max"), rs.getLong("modis"), rs.getLong("viirs")));
    }

    /** Quantas celulas tem registro e o valor da contagem no percentil pedido (0-1): define as regioes criticas. */
    public CellDistribution findCellDistribution(Region.Cover cover, LocalDate s, LocalDate e, List<String> sensors,
                                                 double percentile) {
        String sql = """
                SELECT count(*) AS cells, percentile_cont(:pct) WITHIN GROUP (ORDER BY n) AS pct_value
                FROM (SELECT cell, sum(n) AS n
                      FROM daily_h3
                      WHERE {cells}
                        AND acq_date BETWEEN :start AND :end
                        AND sensor IN (:sensors)
                      GROUP BY cell) t
                """;
        MapSqlParameterSource p = withCover(SqlFilters.period(s, e, sensors), cover).addValue("pct", percentile);
        return jdbc.queryForObject(fill(sql, cover), p, (rs, i) -> new CellDistribution(rs.getLong("cells"),
                getDouble(rs, "pct_value")));
    }

    /** Estatisticas por sensor somando as celulas H3 da regiao; a resolucao nominal vem do catalogo sensor. */
    public List<SensorStats> findSensorStats(Region.Cover cover, LocalDate s, LocalDate e, List<String> sensors) {
        String sql = """
                SELECT d.sensor,
                       sum(d.n) AS n,
                       count(DISTINCT d.acq_date) AS days,
                       coalesce(sum(d.frp_sum), 0) AS frp_total,
                       sum(d.frp_sum) / nullif(sum(d.n) FILTER (WHERE d.frp_sum IS NOT NULL), 0) AS frp_mean,
                       max(d.frp_max) AS frp_max,
                       coalesce(sum(d.viirs_conf_low), 0) AS conf_low,
                       coalesce(sum(d.viirs_conf_nominal), 0) AS conf_nominal,
                       coalesce(sum(d.viirs_conf_high), 0) AS conf_high,
                       sum(d.modis_conf_mean * d.n)
                           / nullif(sum(d.n) FILTER (WHERE d.modis_conf_mean IS NOT NULL), 0) AS modis_conf_mean,
                       max(s.nominal_resolution_m) AS resolution_m
                FROM daily_h3 d
                JOIN sensor s ON s.code = d.sensor
                WHERE {dcells}
                  AND d.acq_date BETWEEN :start AND :end
                  AND d.sensor IN (:sensors)
                GROUP BY d.sensor
                ORDER BY d.sensor
                """;
        return jdbc.query(fill(sql, cover), withCover(SqlFilters.period(s, e, sensors), cover), (rs, i) -> {
            // classes l/n/h existem so no VIIRS; no MODIS a fonte traz a media da confianca (0-100)
            Map<String, Long> conf = new LinkedHashMap<>();
            if ("VIIRS".equals(rs.getString("sensor"))) {
                conf.put("low", rs.getLong("conf_low"));
                conf.put("nominal", rs.getLong("conf_nominal"));
                conf.put("high", rs.getLong("conf_high"));
            }
            return new SensorStats(rs.getString("sensor"), rs.getLong("n"), rs.getInt("days"),
                    getDouble(rs, "frp_mean"), getFloat(rs, "frp_max"), rs.getDouble("frp_total"),
                    conf.isEmpty() ? null : conf,
                    getDouble(rs, "modis_conf_mean"), rs.getInt("resolution_m"));
        });
    }

    /** Cobertura: periodo e total de deteccoes por sensor (uma so resolucao, a mais grossa carregada). */
    public List<Coverage> findCoverage() {
        return jdbc.getJdbcTemplate().query("""
                SELECT sensor, min(acq_date) AS min_date, max(acq_date) AS max_date, sum(n) AS n
                FROM daily_h3 WHERE res = (SELECT min(res) FROM daily_h3) GROUP BY sensor ORDER BY sensor
                """, (rs, i) -> new Coverage(rs.getString("sensor"),
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
