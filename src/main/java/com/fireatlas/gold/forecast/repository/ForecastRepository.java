package com.fireatlas.gold.forecast.repository;

import com.fireatlas.gold.bootstrap.repository.ForecastCacheRepository;
import com.fireatlas.gold.forecast.entity.CellForecast;
import com.fireatlas.gold.forecast.entity.ForecastIssue;
import com.fireatlas.gold.forecast.entity.ForecastSummary;
import com.fireatlas.gold.region.entity.Region;
import com.fireatlas.gold.region.repository.TrinoFilters;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.stereotype.Repository;

import java.sql.Connection;
import java.sql.Date;
import java.time.LocalDate;
import java.util.List;
import java.util.function.Function;

/** Consulta exclusivamente a publicacao completa da previsao nativa, sem agregar ou recalcular celulas. */
@Repository("goldForecastRepository")
public class ForecastRepository {
    private static final int CELL_RES = 6;
    private static final String PUBLISHED = " FROM forecast_cache f JOIN forecast_cache_bootstrap b"
            + " ON b.id AND f.snapshot_id=b.active_snapshot";

    private final JdbcTemplate jdbc;
    private final ForecastCacheRepository bootstrap;

    public ForecastRepository(JdbcTemplate jdbc, ForecastCacheRepository bootstrap) {
        this.jdbc = jdbc;
        this.bootstrap = bootstrap;
    }

    public List<ForecastIssue> findIssues() { return read(Snapshot::findIssues); }

    /** Todas as consultas de uma resposta compartilham a mesma publicacao, inclusive durante um refresh. */
    public static final class Snapshot {
        private final JdbcTemplate transaction;

        private Snapshot(JdbcTemplate transaction) { this.transaction = transaction; }

        /** Datas de emissao disponiveis, a mais recente primeiro. */
        public List<ForecastIssue> findIssues() {
            return transaction.query(
                    "SELECT f.issue_date, count(*) AS cells, count(*) FILTER (WHERE f.active) AS active_cells"
                            + PUBLISHED + " GROUP BY f.issue_date ORDER BY f.issue_date DESC",
                    (rs, index) -> new ForecastIssue(rs.getDate("issue_date").toLocalDate(), rs.getLong("cells"),
                            rs.getLong("active_cells")));
        }

        /** Pede limit+1 para o servico sinalizar truncamento; cada linha continua uma celula nativa r6. */
        public List<CellForecast> findForecast(Region.Cover cover, LocalDate issueDate, double minProbability,
                                               boolean onlyActive, int limit) {
            if (empty(cover)) return List.of();
            String sql = "SELECT f.h3_r6, f.h3_r1, f.prob_fire_7d, f.det_24h, f.det_3d, f.det_7d, f.frp_mean_7d,"
                    + " f.modis_brightness_mean_7d, f.viirs_brightness_mean_7d, f.ring1_det_24h, f.ring1_det_7d,"
                    + " f.ring2_det_7d, f.active, f.country_name_pt, f.city_name, f.city_distance_km"
                    + PUBLISHED + " WHERE f.issue_date=? AND " + region(cover) + " AND f.prob_fire_7d>=?"
                    + (onlyActive ? " AND f.active" : "")
                    + " ORDER BY f.prob_fire_7d DESC, f.h3_r6 LIMIT ?";
            return transaction.query(sql, (rs, index) -> new CellForecast(rs.getLong("h3_r6"),
                    rs.getObject("h3_r1", Long.class), rs.getObject("prob_fire_7d", Double.class),
                    rs.getObject("det_24h", Double.class), rs.getObject("det_3d", Double.class),
                    rs.getObject("det_7d", Double.class), rs.getObject("frp_mean_7d", Double.class),
                    rs.getObject("modis_brightness_mean_7d", Double.class),
                    rs.getObject("viirs_brightness_mean_7d", Double.class),
                    rs.getObject("ring1_det_24h", Double.class), rs.getObject("ring1_det_7d", Double.class),
                    rs.getObject("ring2_det_7d", Double.class), rs.getObject("active", Boolean.class),
                    rs.getString("country_name_pt"), rs.getString("city_name"),
                    rs.getObject("city_distance_km", Double.class)), Date.valueOf(issueDate), minProbability, limit + 1);
        }

        /** Totais da regiao na emissao, sem os filtros da lista. */
        public ForecastSummary findSummary(Region.Cover cover, LocalDate issueDate) {
            if (empty(cover)) return new ForecastSummary(0, 0, null, null);
            return transaction.queryForObject(
                    "SELECT count(*) AS cells, count(*) FILTER (WHERE f.active) AS active_cells,"
                            + " max(f.prob_fire_7d) AS max_prob, avg(f.prob_fire_7d) AS mean_prob"
                            + PUBLISHED + " WHERE f.issue_date=? AND " + region(cover),
                    (rs, index) -> new ForecastSummary(rs.getLong("cells"), rs.getLong("active_cells"),
                            rs.getObject("max_prob", Double.class), rs.getObject("mean_prob", Double.class)),
                    Date.valueOf(issueDate));
        }
    }

    private static String region(Region.Cover cover) {
        if (cover.res() < 0 || cover.res() > CELL_RES) throw new IllegalArgumentException("resolucao da previsao invalida");
        String cell = cover.res() < CELL_RES ? parentSql("f.h3_r6", cover.res()) : "f.h3_r6";
        Region.Place place = cover.place();
        if (place == null) return cell + " IN (" + TrinoFilters.cells(cover.cells()) + ")";
        if (cover.res() == CELL_RES) {
            // A previsao exporta seus proprios lugares r6; a grade anual permanece estritamente r0..r5.
            return TrinoFilters.placeCondition(place).replace("pl.", "f.");
        }
        return cell + " IN (SELECT pl.h3_cell FROM annual_h3_places pl WHERE pl.resolution=" + cover.res()
                + " AND " + TrinoFilters.placeCondition(place) + ")";
    }

    /** Mesmas mascaras H3 de parentBits: bits inferiores em 1 e resolucao nos bits 52..55. */
    private static String parentSql(String column, int res) {
        long ones = (1L << (3 * (15 - res))) - 1;
        return "(((" + column + " | " + ones + ") & " + ~(15L << 52) + ") | " + ((long) res << 52) + ")";
    }

    private static boolean empty(Region.Cover cover) {
        return cover.place() == null && cover.cells().isEmpty();
    }

    public <T> T read(Function<Snapshot, T> query) {
        return jdbc.execute((ConnectionCallback<T>) connection -> {
            int isolation = connection.getTransactionIsolation();
            boolean autoCommit = connection.getAutoCommit();
            connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            connection.setAutoCommit(false);
            var transaction = new JdbcTemplate(new SingleConnectionDataSource(connection, true));
            try {
                // O ponteiro publicado, seus marcadores e as linhas compartilham o mesmo snapshot MVCC.
                bootstrap.requireReady(transaction);
                T result = query.apply(new Snapshot(transaction));
                connection.commit();
                return result;
            } finally {
                connection.rollback();
                connection.setAutoCommit(autoCommit);
                connection.setTransactionIsolation(isolation);
            }
        });
    }
}
