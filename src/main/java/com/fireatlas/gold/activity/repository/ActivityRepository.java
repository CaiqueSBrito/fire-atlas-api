package com.fireatlas.gold.activity.repository;

import com.fireatlas.gold.activity.entity.CellActivity;
import com.fireatlas.gold.activity.entity.DayActivity;
import com.fireatlas.gold.activity.entity.MonthActivity;
import com.fireatlas.gold.activity.entity.SatelliteDay;
import com.fireatlas.gold.activity.entity.YearMonthSum;
import com.fireatlas.gold.activity.policy.TemperatureBins;
import com.fireatlas.gold.region.entity.Region;
import com.fireatlas.gold.region.repository.SqlFilters;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * Consultas da atividade de fogo harmonizada (harmonized_cell e harmonized_availability, vindas de
 * gold/harmonized). Os niveis 0 a 5 ja trazem a soma das celulas filhas: cada consulta usa UMA resolucao, a da
 * cobertura, e nunca soma celulas r6.
 */
@Repository("goldActivityRepository")
public class ActivityRepository {

    private final NamedParameterJdbcTemplate jdbc;

    public ActivityRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static MapSqlParameterSource params(Region.Cover cover, LocalDate start, LocalDate end) {
        return SqlFilters.regionParams(new MapSqlParameterSource(), cover).addValue("start", start)
                .addValue("end", end);
    }

    /**
     * Soma por dia local; contributing e o maior numero de satelites contribuintes entre as celulas. Cada faixa de
     * brilho e somada (nula quando nenhuma linha a trouxe) e basis e a base de temperatura do dia.
     */
    public List<DayActivity> findDaily(Region.Cover cover, LocalDate start, LocalDate end) {
        String sql = """
                SELECT day, sum(harmonized) AS harmonized, max(contributing) AS contributing, count(*) AS n,
                       max(basis) AS basis{bins}
                FROM harmonized_cell
                WHERE {cells} AND day BETWEEN :start AND :end
                GROUP BY day
                ORDER BY day
                """.replace("{cells}", SqlFilters.regionSql(cover, "res", "cell")).replace("{bins}", binSums());
        return jdbc.query(sql, params(cover, start, end), (rs, i) -> {
            List<Double> bins = new ArrayList<>();
            for (TemperatureBins.Bin b : TemperatureBins.ALL) bins.add(rs.getObject(b.column(), Double.class));
            return new DayActivity(rs.getObject("day", LocalDate.class), rs.getDouble("harmonized"),
                    rs.getInt("contributing"), rs.getLong("n"), rs.getString("basis"), bins);
        });
    }

    /**
     * Soma do periodo por celula, na resolucao da cobertura, da maior para a menor atividade; so celulas com algum
     * satelite contribuindo. Pede limit+1 para o chamador poder sinalizar truncamento.
     */
    public List<CellActivity> findCells(Region.Cover cover, LocalDate start, LocalDate end, int limit) {
        String sql = """
                SELECT cell, sum(harmonized) AS harmonized, max(contributing) AS contributing,
                       count(DISTINCT day) FILTER (WHERE contributing > 0) AS days_with_data,
                       sum(coalesce(detections_terra, 0) + coalesce(detections_aqua, 0)) AS modis,
                       sum(coalesce(detections_snpp, 0) + coalesce(detections_noaa20, 0)
                           + coalesce(detections_noaa21, 0)) AS viirs
                FROM harmonized_cell
                WHERE {cells} AND day BETWEEN :start AND :end
                GROUP BY cell
                HAVING max(contributing) > 0
                ORDER BY 2 DESC, 1
                LIMIT :limit
                """.replace("{cells}", SqlFilters.regionSql(cover, "res", "cell"));
        return jdbc.query(sql, params(cover, start, end).addValue("limit", limit + 1), (rs, i) -> new CellActivity(
                rs.getLong("cell"), rs.getDouble("harmonized"), rs.getInt("contributing"), rs.getInt("days_with_data"),
                rs.getLong("modis"), rs.getLong("viirs")));
    }

    private static String binSums() {
        StringBuilder sb = new StringBuilder();
        for (TemperatureBins.Bin b : TemperatureBins.ALL) {
            sb.append(", sum(").append(b.column()).append(") AS ").append(b.column());
        }
        return sb.toString();
    }

    /** Soma por mes; dias com dado = dias em que ao menos uma celula teve satelite contribuindo. */
    public List<MonthActivity> findMonthly(Region.Cover cover, LocalDate start, LocalDate end) {
        String sql = """
                SELECT date_trunc('month', day)::date AS month, sum(harmonized) AS harmonized,
                       count(DISTINCT day) FILTER (WHERE contributing > 0) AS days_with_data
                FROM harmonized_cell
                WHERE {cells} AND day BETWEEN :start AND :end
                GROUP BY 1
                ORDER BY 1
                """.replace("{cells}", SqlFilters.regionSql(cover, "res", "cell"));
        return jdbc.query(sql, params(cover, start, end), (rs, i) -> new MonthActivity(
                rs.getObject("month", LocalDate.class), rs.getDouble("harmonized"), rs.getInt("days_with_data")));
    }

    /** Soma do mesmo mes do calendario em cada ano do intervalo (so anos com linhas), do mais recente ao mais antigo. */
    public List<YearMonthSum> findSameMonthByYear(Region.Cover cover, int month, int fromYear, int toYear) {
        String sql = """
                SELECT extract(year FROM day)::int AS year, sum(harmonized) AS harmonized,
                       count(DISTINCT day) FILTER (WHERE contributing > 0) AS days_with_data
                FROM harmonized_cell
                WHERE {cells} AND extract(month FROM day) = :month
                  AND day BETWEEN :start AND :end
                GROUP BY 1
                ORDER BY 1 DESC
                """.replace("{cells}", SqlFilters.regionSql(cover, "res", "cell"));
        MapSqlParameterSource p = params(cover, LocalDate.of(fromYear, 1, 1), LocalDate.of(toYear, 12, 31))
                .addValue("month", month);
        return jdbc.query(sql, p, (rs, i) -> new YearMonthSum(rs.getInt("year"), rs.getDouble("harmonized"),
                rs.getInt("days_with_data")));
    }

    /** Quantas celulas a regiao tem na resolucao da cobertura (base da estimativa de area). */
    public long countRegionCells(Region.Cover cover) {
        if (cover.place() == null) return cover.cells().size();
        MapSqlParameterSource p = SqlFilters.regionParams(new MapSqlParameterSource(), cover);
        Long n = jdbc.queryForObject("SELECT count(*) FROM h3_place pl WHERE pl.res = :res AND "
                + SqlFilters.placeCondition(cover.place()), p, Long.class);
        return n == null ? 0 : n;
    }

    public List<SatelliteDay> findAvailability(LocalDate start, LocalDate end, String platform) {
        MapSqlParameterSource p = new MapSqlParameterSource().addValue("start", start).addValue("end", end)
                .addValue("platform", platform);
        return jdbc.query("""
                SELECT day, platform, units, typical_units, available, contributes
                FROM harmonized_availability
                WHERE day BETWEEN :start AND :end
                  AND (CAST(:platform AS text) IS NULL OR platform = :platform)
                ORDER BY day, platform
                """, p, (rs, i) -> new SatelliteDay(rs.getObject("day", LocalDate.class), rs.getString("platform"),
                getInt(rs, "units"), getDouble(rs, "typical_units"), getBoolean(rs, "available"),
                getBoolean(rs, "contributes")));
    }

    private static Integer getInt(ResultSet rs, String col) throws SQLException {
        int v = rs.getInt(col);
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
