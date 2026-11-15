package com.fireatlas.gold.bootstrap.repository;

import com.fireatlas.gold.region.entity.Region;
import com.fireatlas.gold.region.repository.TrinoFilters;
import com.fireatlas.lake.integration.AnnualCacheSource;
import com.fireatlas.lake.integration.AthenaQuery;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.stereotype.Repository;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Uma agregacao sobre o conjunto completo: cache de 2026 + linhas externas quando o periodo cruza anos. */
@Repository
public class AnnualCacheQuery {
    private static final int YEAR = AnnualCacheSource.YEAR;
    private static final String COLUMNS = "resolution, year, " + String.join(", ", AnnualCacheSource.GRID_COLUMNS);
    private static final List<String> COLUMN_NAMES = java.util.stream.Stream.concat(
            java.util.stream.Stream.of("resolution", "year"), AnnualCacheSource.GRID_COLUMNS.stream()).toList();
    private static final String MIXED_INSERT = "INSERT INTO annual_query_grid (" + COLUMNS + ") VALUES ("
            + COLUMN_NAMES.stream().map(column -> switch (column) {
                case "sensor" -> "?";
                case "day" -> "CAST(? AS date)";
                case "resolution", "year" -> "CAST(? AS integer)";
                case "h3_cell", "detections", "viirs_confidence_low", "viirs_confidence_nominal", "viirs_confidence_high" -> "CAST(? AS bigint)";
                default -> "CAST(? AS double precision)";
            }).collect(Collectors.joining(", ")) + ")";

    private final AthenaQuery athena;
    private final JdbcTemplate jdbc;
    private final BootstrapRepository bootstrap;

    public AnnualCacheQuery(AthenaQuery athena, JdbcTemplate jdbc, BootstrapRepository bootstrap) {
        this.athena = athena;
        this.jdbc = jdbc;
        this.bootstrap = bootstrap;
    }

    public List<AthenaQuery.Row> grid(String sql, Region.Cover cover, LocalDate start, LocalDate end,
                                     List<String> sensors) {
        if (end.getYear() < YEAR || start.getYear() > YEAR) return athena.run(sql);
        return cached(transaction -> {
            String relation = "annual_map_grid";
            if (start.getYear() < YEAR || end.getYear() > YEAR) {
                // Nunca une rankings/percentis ja limitados: todas as linhas externas entram antes da agregacao.
                transaction.execute("CREATE TEMP TABLE annual_query_grid ON COMMIT DROP AS SELECT " + COLUMNS
                        + " FROM annual_map_grid WITH NO DATA");
                String sensorFilter = sensors.stream().distinct().sorted().map(TrinoFilters::text)
                        .collect(Collectors.joining(", "));
                String external = "SELECT " + COLUMNS + " FROM map_grid WHERE "
                        + TrinoFilters.where(cover, "h3_cell", start, end)
                        + " AND year <> " + YEAR + " AND sensor IN (" + sensorFilter + ")";
                athena.stream(external, page -> transaction.batchUpdate(MIXED_INSERT, page, 1000, (ps, row) -> {
                    for (int i = 0; i < COLUMN_NAMES.size(); i++) ps.setString(i + 1, row.str(COLUMN_NAMES.get(i)));
                }));
                relation = "(SELECT " + COLUMNS + " FROM annual_map_grid UNION ALL SELECT " + COLUMNS
                        + " FROM annual_query_grid) grid";
            }
            return rows(transaction, postgres(sql, relation));
        });
    }

    public List<AthenaQuery.Row> places(String sql) {
        return cached(transaction -> rows(transaction, sql.replace("FROM h3_places", "FROM annual_h3_places")));
    }

    /** Cobertura externa e pequena (uma linha por sensor), nao um download anual por visitante. */
    public List<AthenaQuery.Row> coverage(String sql) {
        List<AthenaQuery.Row> result = new ArrayList<>(cached(transaction -> rows(transaction,
                sql.replace("FROM map_grid", "FROM annual_map_grid"))));
        result.addAll(athena.run(sql.replace("WHERE resolution", "WHERE year <> " + YEAR + " AND resolution")));
        return result;
    }

    private <T> T cached(Function<JdbcTemplate, T> query) {
        return jdbc.execute((ConnectionCallback<T>) connection -> {
            int isolation = connection.getTransactionIsolation();
            boolean autoCommit = connection.getAutoCommit();
            connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            connection.setAutoCommit(false);
            JdbcTemplate transaction = new JdbcTemplate(new SingleConnectionDataSource(connection, true));
            try {
                // O estado e a grade sao lidos no MESMO snapshot, mesmo se outro POST iniciar atualizacao.
                bootstrap.requireReady(transaction);
                T result = query.apply(transaction);
                connection.commit();
                return result;
            } finally {
                connection.rollback();
                connection.setAutoCommit(autoCommit);
                connection.setTransactionIsolation(isolation);
            }
        });
    }

    private static String postgres(String sql, String relation) {
        String converted = sql.replace("FROM map_grid", "FROM " + relation)
                .replace("FROM h3_places", "FROM annual_h3_places");
        // PostgreSQL oferece o percentil exato, sobre TODAS as celulas do conjunto combinado.
        return converted.replaceAll("approx_percentile\\(CAST\\(n AS double\\), ([0-9.Ee+\\-]+)\\)",
                "percentile_cont($1) WITHIN GROUP (ORDER BY CAST(n AS double precision))");
    }

    private static List<AthenaQuery.Row> rows(JdbcTemplate transaction, String sql) {
        return transaction.query(sql, (rs, index) -> {
            ResultSetMetaData metadata = rs.getMetaData();
            LinkedHashMap<String, String> values = new LinkedHashMap<>();
            for (int i = 1; i <= metadata.getColumnCount(); i++) values.put(metadata.getColumnLabel(i), rs.getString(i));
            return new AthenaQuery.Row(values);
        });
    }
}
