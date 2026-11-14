package com.fireatlas.forecast.repository;

import com.fireatlas.forecast.entity.CellForecast;
import com.fireatlas.forecast.entity.ForecastIssue;
import com.fireatlas.region.entity.Region;
import com.fireatlas.region.repository.SqlFilters;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.List;

/** Consultas a previsao (tabela forecast, vinda da gold/forecast). So devolve entidades. */
@Repository
public class ForecastRepository {

    private final NamedParameterJdbcTemplate jdbc;

    public ForecastRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Datas de emissao disponiveis, a mais recente primeiro. */
    public List<ForecastIssue> findIssues() {
        return jdbc.getJdbcTemplate().query("""
                SELECT issue_date, count(*) AS cells, count(*) FILTER (WHERE active) AS active_cells
                FROM forecast GROUP BY issue_date ORDER BY issue_date DESC
                """, (rs, i) -> new ForecastIssue(rs.getObject("issue_date", LocalDate.class), rs.getLong("cells"),
                rs.getLong("active_cells")));
    }

    /**
     * Celulas da regiao (celulas de resolucao 6 ou lugar) na emissao, da maior para a menor probabilidade. Pede
     * limit+1 para o chamador poder sinalizar truncamento.
     */
    public List<CellForecast> findForecast(Region.Cover cover, LocalDate issueDate, double minProbability,
                                           boolean onlyActive, int limit) {
        MapSqlParameterSource p = SqlFilters.regionParams(new MapSqlParameterSource(), cover)
                .addValue("issue", issueDate).addValue("minProb", minProbability).addValue("onlyActive", onlyActive)
                .addValue("limit", limit + 1);
        String sql = """
                SELECT h3_r6, h3_r1, prob_fire_7d, det_24h, det_3d, det_7d, frp_mean_7d, modis_brightness_mean_7d,
                       viirs_brightness_mean_7d, ring1_det_24h, ring1_det_7d, ring2_det_7d, active
                FROM forecast f
                WHERE f.issue_date = :issue AND {region}
                  AND f.prob_fire_7d >= :minProb
                  AND (NOT :onlyActive OR f.active)
                ORDER BY f.prob_fire_7d DESC, f.h3_r6
                LIMIT :limit
                """.replace("{region}", SqlFilters.regionSqlFixedRes(cover, "f.h3_r6"));
        return jdbc.query(sql, p, (rs, i) -> new CellForecast(rs.getLong("h3_r6"), getLong(rs, "h3_r1"),
                getDouble(rs, "prob_fire_7d"), getDouble(rs, "det_24h"), getDouble(rs, "det_3d"),
                getDouble(rs, "det_7d"), getDouble(rs, "frp_mean_7d"), getDouble(rs, "modis_brightness_mean_7d"),
                getDouble(rs, "viirs_brightness_mean_7d"), getDouble(rs, "ring1_det_24h"),
                getDouble(rs, "ring1_det_7d"), getDouble(rs, "ring2_det_7d"), getBoolean(rs, "active")));
    }

    private static Long getLong(ResultSet rs, String col) throws SQLException {
        long v = rs.getLong(col);
        return rs.wasNull() ? null : v;
    }

    private static Double getDouble(ResultSet rs, String col) throws SQLException {
        double v = rs.getDouble(col);
        return rs.wasNull() ? null : v;
    }

    private static Boolean getBoolean(ResultSet rs, String col) throws SQLException {
        boolean v = rs.getBoolean(col);
        return rs.wasNull() ? null : v;
    }
}
