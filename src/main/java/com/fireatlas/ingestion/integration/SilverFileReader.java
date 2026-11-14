package com.fireatlas.ingestion.integration;

import com.fireatlas.region.entity.BBox;

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
 * Le um arquivo Parquet da prata ({@code prata/silver}: um foco por linha, com H3 e rastreabilidade) e o mapeia para
 * o esquema da tabela {@code detection}, gravando as linhas validas em CSV para a carga em lote. Nada e calculado:
 * H3, proveniencia e valores originais vem da prata.
 * <p>
 * Mapeamento: sensor, satelite (codigos curtos da FIRMS: T, A, N, N20, N21), instante UTC, posicao, brilho (VIIRS:
 * I-4 e I-5; MODIS: canais 21/22 e 31), scan e track em km, FRP em MW, confianca original (MODIS 0-100; VIIRS l/n/h) e
 * a classe auxiliar l/n/h pela mesma regra do CSV, dia/noite, versao, produto e proveniencia (arquivo de origem,
 * numero da linha, sha256 e record_id).
 * <p>
 * Linhas rejeitadas, com o motivo contado: {@code record_id_ausente}, {@code sensor_invalido}, {@code satelite_ausente},
 * {@code coordenada_invalida}, {@code instante_ausente} e {@code frp_negativo} (decisao da equipe).
 */
public final class SilverFileReader {

    static final Set<String> REQUIRED = Set.of("record_id", "sensor", "platform", "latitude", "longitude",
            "acquisition_date", "acquired_at", "source_product");

    /** Colunas do CSV gravado, na ordem esperada pela carga de staging. */
    public static final String STAGING_COLUMNS = "sensor, satellite, acq_date, acq_ts, lat, lon, brightness, "
            + "bright_t31, scan, track, frp, confidence_raw, confidence_class, daynight, version, source_product, "
            + "source_file, source_line_number, source_file_sha256, record_id, h3_r6";

    /** Resultado: linhas lidas no recorte, gravadas, rejeitadas (por motivo) e dias cobertos. */
    public record Result(long scanned, long loaded, Map<String, Long> rejects, LocalDate minDate, LocalDate maxDate) {
        public long rejected() {
            return rejects.values().stream().mapToLong(Long::longValue).sum();
        }
    }

    private SilverFileReader() {}

    /** bbox nulo = sem filtro de regiao (o arquivo e lido por inteiro dentro do periodo). */
    public static Result extract(Path parquet, LocalDate start, LocalDate end, BBox bbox, Path out)
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
            String region = bbox == null ? "" : " AND latitude BETWEEN " + bbox.minLat() + " AND " + bbox.maxLat()
                    + " AND longitude BETWEEN " + bbox.minLon() + " AND " + bbox.maxLon();
            String negativeFrp = cols.contains("frp_mw") ? "WHEN frp_mw < 0 THEN 'frp_negativo' " : "";
            s.execute("CREATE TEMP VIEW src AS SELECT *, CASE "
                    + "WHEN record_id IS NULL OR trim(record_id) = '' THEN 'record_id_ausente' "
                    + "WHEN sensor IS NULL OR upper(sensor) NOT IN ('MODIS', 'VIIRS') THEN 'sensor_invalido' "
                    + "WHEN platform IS NULL OR trim(platform) = '' THEN 'satelite_ausente' "
                    + "WHEN latitude IS NULL OR longitude IS NULL OR latitude < -90 OR latitude > 90 "
                    + "OR longitude < -180 OR longitude > 180 THEN 'coordenada_invalida' "
                    + "WHEN acquired_at IS NULL OR acquisition_date IS NULL THEN 'instante_ausente' "
                    + negativeFrp + "END AS reason FROM read_parquet(" + file + ") "
                    + "WHERE (acquisition_date IS NULL OR acquisition_date BETWEEN DATE '" + start + "' AND DATE '"
                    + end + "')" + region);

            long scanned = 0;
            long loaded = 0;
            Map<String, Long> rejects = new LinkedHashMap<>();
            LocalDate min = null;
            LocalDate max = null;
            try (ResultSet rs = s.executeQuery("SELECT reason, count(*), min(acquisition_date), "
                    + "max(acquisition_date) FROM src GROUP BY reason ORDER BY reason NULLS FIRST")) {
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
                s.execute("COPY (SELECT upper(sensor) AS sensor, " + satellite() + " AS satellite, "
                        + "acquisition_date AS acq_date, acquired_at AS acq_ts, latitude AS lat, longitude AS lon, "
                        + "coalesce(" + opt(cols, "brightness_i4_k") + ", " + opt(cols, "brightness_k")
                        + ") AS brightness, coalesce(" + opt(cols, "bright_t31_k") + ", " + opt(cols, "brightness_i5_k")
                        + ") AS bright_t31, " + opt(cols, "scan_km") + " AS scan, " + opt(cols, "track_km")
                        + " AS track, " + opt(cols, "frp_mw") + " AS frp, " + confidenceRaw(cols) + " AS confidence_raw, "
                        + confidenceClass(cols) + " AS confidence_class, "
                        + "upper(substr(CAST(" + opt(cols, "daynight") + " AS VARCHAR), 1, 1)) AS daynight, "
                        + opt(cols, "source_version") + " AS version, source_product, "
                        + "'prata/silver/' || coalesce(CAST(" + opt(cols, "source_sha256") + " AS VARCHAR), 'sem-sha256') "
                        + "AS source_file, " + opt(cols, "source_record_number") + " AS source_line_number, "
                        + opt(cols, "source_sha256") + " AS source_file_sha256, record_id, " + opt(cols, "h3_r6")
                        + " AS h3_r6 FROM src WHERE reason IS NULL) TO "
                        + lit(out.toAbsolutePath().toString().replace("\\", "/")) + " (FORMAT csv, HEADER false)");
            }
            return new Result(scanned, loaded, rejects, min, max);
        } catch (SQLException e) {
            throw new IOException("Parquet invalido ou ilegivel: " + e.getMessage(), e);
        }
    }

    /** Plataforma da prata -> codigo curto da FIRMS usado na detection (T, A, N, N20, N21); outro valor segue como veio. */
    private static String satellite() {
        return "CASE upper(trim(platform)) WHEN 'TERRA' THEN 'T' WHEN 'AQUA' THEN 'A' WHEN 'NOAA-20' THEN 'N20' "
                + "WHEN 'NOAA-21' THEN 'N21' WHEN 'S-NPP' THEN 'N' WHEN 'SNPP' THEN 'N' WHEN 'SUOMI NPP' THEN 'N' "
                + "WHEN 'SUOMI-NPP' THEN 'N' ELSE trim(platform) END";
    }

    /** Confianca original: MODIS em percentual (0-100) e VIIRS na categoria l/n/h, como a FIRMS publica. */
    private static String confidenceRaw(Set<String> cols) {
        return "CASE WHEN upper(sensor) = 'MODIS' THEN CAST(" + opt(cols, "confidence_percent") + " AS VARCHAR) "
                + "ELSE " + opt(cols, "confidence_category") + " END";
    }

    /** Classe auxiliar l/n/h pela mesma regra do CSV (MODIS 0-29 l, 30-79 n, 80-100 h; VIIRS l/n/h); desconhecido = nulo. */
    private static String confidenceClass(Set<String> cols) {
        String pct = opt(cols, "confidence_percent");
        String cat = "lower(CAST(" + opt(cols, "confidence_category") + " AS VARCHAR))";
        return "CASE WHEN upper(sensor) = 'MODIS' THEN (CASE WHEN " + pct + " IS NULL OR " + pct + " < 0 OR " + pct
                + " > 100 THEN NULL WHEN " + pct + " >= 80 THEN 'h' WHEN " + pct + " >= 30 THEN 'n' ELSE 'l' END) "
                + "ELSE (CASE " + cat + " WHEN 'l' THEN 'l' WHEN 'low' THEN 'l' WHEN 'n' THEN 'n' "
                + "WHEN 'nominal' THEN 'n' WHEN 'h' THEN 'h' WHEN 'high' THEN 'h' ELSE NULL END) END";
    }

    private static Set<String> columns(Statement s, String file) throws SQLException {
        Set<String> cols = new HashSet<>();
        try (ResultSet rs = s.executeQuery("DESCRIBE SELECT * FROM read_parquet(" + file + ")")) {
            while (rs.next()) cols.add(rs.getString("column_name"));
        }
        return cols;
    }

    /** Coluna opcional: se o arquivo nao a tem, vai nula (nunca inventada). */
    private static String opt(Set<String> cols, String name) {
        return cols.contains(name) ? name : "NULL";
    }

    private static String lit(String text) {
        return "'" + text.replace("'", "''") + "'";
    }
}
