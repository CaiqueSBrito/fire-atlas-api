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
 * Le um arquivo Parquet de {@code gold/harmonized/availability} (disponibilidade de cada satelite por dia), valida
 * cada linha e grava as validas em CSV. Linhas rejeitadas: {@code data_ausente} e {@code satelite_ausente}.
 */
public final class AvailabilityFileReader {

    static final Set<String> REQUIRED = Set.of("day", "platform");

    /** Colunas do CSV gravado, na ordem esperada pela carga em harmonized_availability. */
    public static final String COPY_COLUMNS = "day, platform, units, typical_units, available, contributes, "
            + "source_key";

    private AvailabilityFileReader() {}

    public static GridFileReader.Result extract(Path parquet, String sourceKey, LocalDate start, LocalDate end,
                                                Path out) throws IOException {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement s = c.createStatement()) {
            String file = lit(parquet.toAbsolutePath().toString().replace("\\", "/"));
            Set<String> cols = new HashSet<>();
            try (ResultSet rs = s.executeQuery("DESCRIBE SELECT * FROM read_parquet(" + file + ")")) {
                while (rs.next()) cols.add(rs.getString("column_name"));
            }
            if (!cols.containsAll(REQUIRED)) {
                throw new IOException("arquivo sem colunas obrigatorias " + REQUIRED + " (colunas: "
                        + new java.util.TreeSet<>(cols) + ")");
            }
            s.execute("CREATE TEMP VIEW src AS SELECT *, CASE "
                    + "WHEN day IS NULL THEN 'data_ausente' "
                    + "WHEN platform IS NULL OR trim(platform) = '' THEN 'satelite_ausente' END AS reason "
                    + "FROM read_parquet(" + file + ") "
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
                s.execute("COPY (SELECT day, platform, " + opt(cols, "units", "INTEGER") + ", "
                        + opt(cols, "typical_units", "DOUBLE") + ", " + opt(cols, "available", "BOOLEAN") + ", "
                        + opt(cols, "contributes", "BOOLEAN") + ", " + lit(sourceKey)
                        + " AS source_key FROM src WHERE reason IS NULL) TO "
                        + lit(out.toAbsolutePath().toString().replace("\\", "/")) + " (FORMAT csv, HEADER false)");
            }
            return new GridFileReader.Result(scanned, loaded, rejects, min, max);
        } catch (SQLException e) {
            throw new IOException("Parquet invalido ou ilegivel: " + e.getMessage(), e);
        }
    }

    private static String opt(Set<String> cols, String name, String type) {
        return cols.contains(name) ? "CAST(" + name + " AS " + type + ")" : "CAST(NULL AS " + type + ")";
    }

    private static String lit(String text) {
        return "'" + text.replace("'", "''") + "'";
    }
}
