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
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Le um arquivo Parquet de {@code gold/harmonized/cells} (atividade harmonizada por celula H3, dia local e nivel de
 * zoom), valida cada linha e grava as validas em CSV para a carga em lote. O numero harmonizado vem pronto do
 * pipeline de dados: a API nao recalcula nada.
 * <p>
 * Linhas rejeitadas, com o motivo contado: {@code data_ausente}, {@code h3_invalido} (ausente ou de resolucao
 * diferente da particao), {@code harmonizado_invalido} (ausente ou negativo) e {@code contribuintes_invalidos}
 * (ausente ou fora de 0 a 5). Atencao: {@code contributing = 0} e valido e significa SEM DADO.
 */
public final class HarmonizedFileReader {

    /** Satelites das colunas units_, detections_ e aqua_eq_. */
    public static final List<String> SATELLITES = List.of("terra", "aqua", "snpp", "noaa20", "noaa21");

    static final Set<String> REQUIRED = Set.of("h3_cell", "day", "harmonized", "contributing");

    /** Colunas do CSV gravado, na ordem esperada pela carga em harmonized_cell. */
    public static final String COPY_COLUMNS = "res, cell, day, harmonized, contributing, " + satelliteColumns()
            + ", source_key";

    private HarmonizedFileReader() {}

    private static String satelliteColumns() {
        StringBuilder sb = new StringBuilder();
        for (String s : SATELLITES) {
            if (sb.length() > 0) sb.append(", ");
            sb.append("units_").append(s).append(", detections_").append(s).append(", aqua_eq_").append(s);
        }
        return sb.toString();
    }

    public static GridFileReader.Result extract(Path parquet, int resolution, String sourceKey, LocalDate start,
                                                LocalDate end, Path out) throws IOException {
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
                    + "WHEN day IS NULL THEN 'data_ausente' "
                    + "WHEN h3_cell IS NULL OR ((h3_cell >> 52) & 15) <> " + resolution + " THEN 'h3_invalido' "
                    + "WHEN harmonized IS NULL OR isnan(harmonized) OR harmonized < 0 THEN 'harmonizado_invalido' "
                    + "WHEN contributing IS NULL OR contributing < 0 OR contributing > 5 "
                    + "THEN 'contribuintes_invalidos' END AS reason FROM read_parquet(" + file + ") "
                    + "WHERE day IS NULL OR day BETWEEN DATE '" + start + "' AND DATE '" + end + "'");

            long scanned = 0;
            long loaded = 0;
            Map<String, Long> rejects = new LinkedHashMap<>();
            LocalDate min = null;
            LocalDate max = null;
            try (ResultSet rs = s.executeQuery("SELECT reason, count(*), min(day), max(day) FROM src GROUP BY reason "
                    + "ORDER BY reason NULLS FIRST")) {
                while (rs.next()) {
                    long n = rs.getLong(2);
                    scanned += n;
                    if (rs.getString(1) == null) {
                        loaded = n;
                        min = LocalDate.parse(rs.getString(3));
                        max = LocalDate.parse(rs.getString(4));
                    } else {
                        rejects.put(rs.getString(1), n);
                    }
                }
            }
            if (loaded > 0) {
                StringBuilder sats = new StringBuilder();
                for (String sat : SATELLITES) {
                    sats.append(", ").append(opt(cols, "units_" + sat, "INTEGER"))
                            .append(", ").append(opt(cols, "detections_" + sat, "INTEGER"))
                            .append(", ").append(opt(cols, "aqua_eq_" + sat, "DOUBLE"));
                }
                s.execute("COPY (SELECT " + resolution + " AS res, h3_cell, day, harmonized, "
                        + "CAST(contributing AS SMALLINT)" + sats + ", " + lit(sourceKey)
                        + " AS source_key FROM src WHERE reason IS NULL) TO "
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
        return cols.contains(name) ? "CAST(" + name + " AS " + type + ")" : "CAST(NULL AS " + type + ")";
    }

    private static String lit(String text) {
        return "'" + text.replace("'", "''") + "'";
    }
}
