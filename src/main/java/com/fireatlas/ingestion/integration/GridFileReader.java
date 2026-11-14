package com.fireatlas.ingestion.integration;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Le um arquivo Parquet da gold (grade H3: uma linha por celula, dia e sensor), valida cada linha e grava as validas
 * em um CSV temporario para a carga em lote. Nada e calculado nem convertido: o indice H3 e os agregados vem
 * prontos da fonte. Usa DuckDB embutido; o filtro de periodo e aplicado na leitura.
 * <p>
 * Linhas rejeitadas (RN03), com o motivo contado: {@code data_ausente}, {@code sensor_invalido} (so MODIS e VIIRS),
 * {@code h3_invalido} (ausente ou de resolucao diferente da particao do arquivo), {@code deteccoes_invalidas}
 * (ausente ou menor que 1) e {@code frp_negativo} (decisao da equipe: FRP negativo nao entra).
 */
public final class GridFileReader {

    /** Colunas que o arquivo precisa ter; as demais sao opcionais (ficam nulas se faltarem). */
    static final Set<String> REQUIRED = Set.of("h3_cell", "day", "sensor", "detections", "frp_sum_mw");

    /** Resultado da leitura: linhas lidas no periodo, gravadas no CSV, rejeitadas (por motivo) e dias cobertos. */
    public record Result(long scanned, long loaded, Map<String, Long> rejects, LocalDate minDay, LocalDate maxDay) {
        public long rejected() {
            return rejects.values().stream().mapToLong(Long::longValue).sum();
        }
    }

    /** Colunas do CSV gravado, na ordem esperada pela carga em daily_h3. */
    public static final String COPY_COLUMNS = "res, cell, acq_date, sensor, n, frp_sum, frp_mean, platforms, frp_max, "
            + "brightness_max, viirs_conf_low, viirs_conf_nominal, viirs_conf_high, modis_conf_mean, source_key";

    private GridFileReader() {}

    /**
     * @param parquet    arquivo local
     * @param resolution resolucao H3 da particao (as celulas do arquivo devem ser desta resolucao)
     * @param sourceKey  proveniencia gravada em cada linha
     * @param out        CSV de saida (sem cabecalho)
     */
    public static Result extract(Path parquet, int resolution, String sourceKey, LocalDate start, LocalDate end,
                                 Path out) throws IOException {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement s = c.createStatement()) {
            String file = literal(parquet.toAbsolutePath().toString().replace("\\", "/"));
            Set<String> cols = columns(s, file);
            if (!cols.containsAll(REQUIRED)) {
                Set<String> missing = new HashSet<>(REQUIRED);
                missing.removeAll(cols);
                throw new IOException("arquivo sem colunas obrigatorias " + new java.util.TreeSet<>(missing)
                        + " (colunas: " + new java.util.TreeSet<>(cols) + ")");
            }
            String where = "day IS NULL OR day BETWEEN DATE '" + start + "' AND DATE '" + end + "'";
            s.execute("CREATE TEMP VIEW src AS SELECT *, CASE "
                    + "WHEN day IS NULL THEN 'data_ausente' "
                    + "WHEN sensor IS NULL OR sensor NOT IN ('MODIS', 'VIIRS') THEN 'sensor_invalido' "
                    + "WHEN h3_cell IS NULL OR ((h3_cell >> 52) & 15) <> " + resolution + " THEN 'h3_invalido' "
                    + "WHEN detections IS NULL OR detections < 1 THEN 'deteccoes_invalidas' "
                    + "WHEN frp_sum_mw < 0 " + (cols.contains("frp_max_mw") ? "OR frp_max_mw < 0 " : "")
                    + "THEN 'frp_negativo' END AS reason FROM read_parquet(" + file + ") WHERE " + where);

            long scanned = 0;
            long loaded = 0;
            Map<String, Long> rejects = new LinkedHashMap<>();
            LocalDate min = null;
            LocalDate max = null;
            try (ResultSet rs = s.executeQuery("SELECT reason, count(*), min(day), max(day) FROM src GROUP BY reason "
                    + "ORDER BY reason NULLS FIRST")) {
                while (rs.next()) {
                    String reason = rs.getString(1);
                    long n = rs.getLong(2);
                    scanned += n;
                    if (reason == null) {
                        loaded = n;
                        min = LocalDate.parse(rs.getString(3));
                        max = LocalDate.parse(rs.getString(4));
                    } else {
                        rejects.put(reason, n);
                    }
                }
            }
            if (loaded > 0) {
                s.execute("COPY (SELECT " + resolution + " AS res, h3_cell, day, sensor, detections, frp_sum_mw, "
                        + opt(cols, "frp_mean_mw", "DOUBLE") + ", " + opt(cols, "platforms", "INTEGER") + ", "
                        + opt(cols, "frp_max_mw", "DOUBLE") + ", " + opt(cols, "brightness_max_k", "DOUBLE") + ", "
                        + opt(cols, "viirs_confidence_low", "BIGINT") + ", "
                        + opt(cols, "viirs_confidence_nominal", "BIGINT") + ", "
                        + opt(cols, "viirs_confidence_high", "BIGINT") + ", "
                        + opt(cols, "modis_confidence_mean", "DOUBLE") + ", " + literal(sourceKey)
                        + " AS source_key FROM src WHERE reason IS NULL) TO "
                        + literal(out.toAbsolutePath().toString().replace("\\", "/"))
                        + " (FORMAT csv, HEADER false)");
            }
            return new Result(scanned, loaded, rejects, min, max);
        } catch (SQLException e) {
            throw new IOException("Parquet invalido ou ilegivel: " + e.getMessage(), e);
        }
    }

    private static Set<String> columns(Statement s, String file) throws SQLException {
        Set<String> cols = new HashSet<>();
        try (ResultSet rs = s.executeQuery("DESCRIBE SELECT * FROM read_parquet(" + file + ")")) {
            while (rs.next()) cols.add(rs.getString("column_name"));
        }
        return cols;
    }

    /** Coluna opcional: se o arquivo nao a tem, vai nula (nunca inventada). */
    private static String opt(Set<String> cols, String name, String type) {
        return cols.contains(name) ? name : "CAST(NULL AS " + type + ")";
    }

    private static String literal(String text) {
        return "'" + text.replace("'", "''") + "'";
    }
}
