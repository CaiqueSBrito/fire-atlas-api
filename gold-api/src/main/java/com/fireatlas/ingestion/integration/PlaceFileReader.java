package com.fireatlas.ingestion.integration;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Le um arquivo Parquet da tabela {@code gold/h3_places} (uma linha por celula H3 de uma resolucao: continente,
 * pais predominante e cidade mais proxima), valida cada linha e grava as validas em CSV para a carga em lote.
 * Nada e calculado: continente, pais e cidade vem prontos do pipeline de dados.
 * <p>
 * Linhas rejeitadas, com o motivo contado: {@code h3_invalido} (ausente ou de resolucao diferente da particao) e
 * {@code share_invalido} (fracao do pais fora de 0 a 1). Celulas sem pais (oceano) sao validas e ficam sem pais.
 */
public final class PlaceFileReader {

    static final Set<String> REQUIRED = Set.of("h3_cell");

    /** Colunas do CSV gravado, na ordem esperada pela carga em h3_place. */
    public static final String COPY_COLUMNS = "res, cell, latitude, longitude, continent, country_code, "
            + "country_iso_a2, country_name, country_name_pt, country_share, country_method, detections, "
            + "city_geonameid, city_name, city_country_code, city_distance_km, source_key";

    private PlaceFileReader() {}

    public static GridFileReader.Result extract(Path parquet, int resolution, String sourceKey, Path out)
            throws IOException {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement s = c.createStatement()) {
            String file = lit(parquet.toAbsolutePath().toString().replace("\\", "/"));
            Set<String> cols = columns(s, file);
            if (!cols.containsAll(REQUIRED)) {
                throw new IOException("arquivo sem colunas obrigatorias " + REQUIRED + " (colunas: "
                        + new java.util.TreeSet<>(cols) + ")");
            }
            String share = cols.contains("country_share")
                    ? "WHEN country_share IS NOT NULL AND (country_share < 0 OR country_share > 1) THEN 'share_invalido' "
                    : "";
            s.execute("CREATE TEMP VIEW src AS SELECT *, CASE "
                    + "WHEN h3_cell IS NULL OR ((h3_cell >> 52) & 15) <> " + resolution + " THEN 'h3_invalido' "
                    + share + "END AS reason FROM read_parquet(" + file + ")");

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
            if (loaded > 0) {
                s.execute("COPY (SELECT " + resolution + " AS res, h3_cell, " + opt(cols, "latitude", "DOUBLE") + ", "
                        + opt(cols, "longitude", "DOUBLE") + ", " + opt(cols, "continent", "VARCHAR") + ", "
                        + opt(cols, "country_code", "VARCHAR") + ", " + opt(cols, "country_iso_a2", "VARCHAR") + ", "
                        + opt(cols, "country_name", "VARCHAR") + ", " + opt(cols, "country_name_pt", "VARCHAR") + ", "
                        + opt(cols, "country_share", "DOUBLE") + ", " + opt(cols, "country_method", "VARCHAR") + ", "
                        + opt(cols, "detections", "BIGINT") + ", " + opt(cols, "city_geonameid", "BIGINT") + ", "
                        + opt(cols, "city_name", "VARCHAR") + ", " + opt(cols, "city_country_code", "VARCHAR") + ", "
                        + opt(cols, "city_distance_km", "DOUBLE") + ", " + lit(sourceKey)
                        + " AS source_key FROM src WHERE reason IS NULL) TO "
                        + lit(out.toAbsolutePath().toString().replace("\\", "/")) + " (FORMAT csv, HEADER false)");
            }
            return new GridFileReader.Result(scanned, loaded, rejects, null, null);
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
