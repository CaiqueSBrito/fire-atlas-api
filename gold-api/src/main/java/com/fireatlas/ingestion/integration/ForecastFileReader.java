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
 * Le um arquivo Parquet da tabela {@code gold/forecast} (previsao de fogo por celula H3 de resolucao 6 e data de
 * emissao), valida cada linha e grava as validas em CSV para a carga em lote. O valor previsto vem pronto do
 * pipeline de dados: a API nao recalcula nada.
 * <p>
 * Linhas rejeitadas, com o motivo contado: {@code data_ausente}, {@code h3_invalido} (ausente ou nao de resolucao 6)
 * e {@code probabilidade_invalida} (ausente ou fora de 0 a 1: previsao sem numero nao e inventada).
 */
public final class ForecastFileReader {

    static final Set<String> REQUIRED = Set.of("issue_date", "h3_r6", "prob_fire_7d");

    /** Colunas do CSV gravado, na ordem esperada pela carga em forecast. */
    public static final String COPY_COLUMNS = "issue_date, h3_r6, h3_r1, det_24h, det_3d, det_7d, frp_mean_7d, "
            + "modis_brightness_mean_7d, viirs_brightness_mean_7d, ring1_det_24h, ring1_det_7d, ring2_det_7d, "
            + "active, prob_fire_7d, source_key";

    private ForecastFileReader() {}

    /** Datas de emissao do arquivo (para apagar antes de recarregar) vao em {@code issueDates}. */
    public static GridFileReader.Result extract(Path parquet, String sourceKey, Path out, Set<LocalDate> issueDates)
            throws IOException {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement s = c.createStatement()) {
            String file = lit(parquet.toAbsolutePath().toString().replace("\\", "/"));
            Set<String> cols = columns(s, file);
            if (!cols.containsAll(REQUIRED)) {
                Set<String> missing = new HashSet<>(REQUIRED);
                missing.removeAll(cols);
                throw new IOException("arquivo sem colunas obrigatorias " + new java.util.TreeSet<>(missing)
                        + " (colunas: " + new java.util.TreeSet<>(cols) + ")");
            }
            s.execute("CREATE TEMP VIEW src AS SELECT *, CASE "
                    + "WHEN issue_date IS NULL THEN 'data_ausente' "
                    + "WHEN h3_r6 IS NULL OR ((h3_r6 >> 52) & 15) <> 6 THEN 'h3_invalido' "
                    + "WHEN prob_fire_7d IS NULL OR prob_fire_7d < 0 OR prob_fire_7d > 1 THEN 'probabilidade_invalida' "
                    + "END AS reason FROM read_parquet(" + file + ")");

            long scanned = 0;
            long loaded = 0;
            Map<String, Long> rejects = new LinkedHashMap<>();
            try (ResultSet rs = s.executeQuery("SELECT reason, count(*) FROM src GROUP BY reason "
                    + "ORDER BY reason NULLS FIRST")) {
                while (rs.next()) {
                    long n = rs.getLong(2);
                    scanned += n;
                    if (rs.getString(1) == null) loaded = n;
                    else rejects.put(rs.getString(1), n);
                }
            }
            LocalDate min = null;
            LocalDate max = null;
            try (ResultSet rs = s.executeQuery("SELECT DISTINCT issue_date FROM src WHERE issue_date IS NOT NULL "
                    + "ORDER BY 1")) {
                while (rs.next()) {
                    LocalDate d = LocalDate.parse(rs.getString(1));
                    issueDates.add(d);
                    if (min == null) min = d;
                    max = d;
                }
            }
            if (loaded > 0) {
                s.execute("COPY (SELECT issue_date, h3_r6, " + opt(cols, "h3_r1", "BIGINT") + ", "
                        + opt(cols, "det_24h", "DOUBLE") + ", " + opt(cols, "det_3d", "DOUBLE") + ", "
                        + opt(cols, "det_7d", "DOUBLE") + ", " + opt(cols, "frp_mean_7d", "DOUBLE") + ", "
                        + opt(cols, "modis_brightness_mean_7d", "DOUBLE") + ", "
                        + opt(cols, "viirs_brightness_mean_7d", "DOUBLE") + ", "
                        + opt(cols, "ring1_det_24h", "DOUBLE") + ", " + opt(cols, "ring1_det_7d", "DOUBLE") + ", "
                        + opt(cols, "ring2_det_7d", "DOUBLE") + ", " + opt(cols, "active", "BOOLEAN") + ", "
                        + "prob_fire_7d, " + lit(sourceKey) + " AS source_key FROM src WHERE reason IS NULL) TO "
                        + lit(out.toAbsolutePath().toString().replace("\\", "/")) + " (FORMAT csv, HEADER false)");
            }
            return new GridFileReader.Result(scanned, loaded, rejects, min, max);
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

    private static String opt(Set<String> cols, String name, String type) {
        return cols.contains(name) ? name : "CAST(NULL AS " + type + ")";
    }

    private static String lit(String text) {
        return "'" + text.replace("'", "''") + "'";
    }
}
