package com.fireatlas.ingestion;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Somente para testes: fabrica a tabela Delta da prata (um foco por linha, particionada por acquisition_year e
 * acquisition_month) no formato real, com Parquet de verdade e o _delta_log escrito a mao. Faz o papel do pipeline do
 * time de dados; o backend so le.
 */
public final class SilverFixtures {

    private SilverFixtures() {}

    /** Linha da prata. Campos nulos viram NULL (ex.: VIIRS sem confidence_percent, MODIS sem categoria). */
    public record Row(String recordId, String sensor, String platform, double lat, double lon, String date,
                      String time, Double brightnessK, Double i4, Double i5, Double t31, Double frp, Double scan,
                      Double track, String dayNight, String confCategory, Integer confPercent, String version,
                      String product, String sha, long recordNo, long h3r6) {

        /** Foco VIIRS: I-4 e I-5, confianca por categoria. */
        public static Row viirs(String sha, long no, String platform, double lat, double lon, String date,
                                String time, double frp, String category, long h3r6) {
            return new Row(sha + ":" + no, "VIIRS", platform, lat, lon, date, time, null, 340.0, 290.0, null, frp, 0.4,
                    0.4, "D", category, null, "2.0NRT", "VIIRS_" + platform.replace("-", "") + "_NRT", sha, no, h3r6);
        }

        /** Foco MODIS: brilho e canal 31, confianca em percentual. */
        public static Row modis(String sha, long no, String platform, double lat, double lon, String date,
                                String time, double frp, int percent, long h3r6) {
            return new Row(sha + ":" + no, "MODIS", platform, lat, lon, date, time, 330.1, null, null, 300.2, frp, 1.0,
                    1.0, "D", null, percent, "6.1NRT", "MODIS_NRT", sha, no, h3r6);
        }

        String month() {
            return String.valueOf(Integer.parseInt(date.substring(5, 7)));
        }

        String year() {
            return date.substring(0, 4);
        }
    }

    /** numRecords que o log do Delta declara; nulo = o numero real de linhas do arquivo. */
    public static void write(Path root, List<Row> rows, Long declaredRecords) throws Exception {
        Path table = root.resolve("prata/silver");
        List<String> log = new ArrayList<>();
        log.add("{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}");
        log.add("{\"metaData\":{\"id\":\"" + UUID.randomUUID() + "\",\"format\":{\"provider\":\"parquet\","
                + "\"options\":{}},\"schemaString\":\"{}\",\"partitionColumns\":[\"acquisition_year\","
                + "\"acquisition_month\"],\"createdTime\":0,\"configuration\":{}}}");
        Map<String, List<Row>> groups = rows.stream().collect(Collectors.groupingBy(
                r -> r.year() + "|" + r.month(), TreeMap::new, Collectors.toList()));
        for (var e : groups.entrySet()) {
            List<Row> part = e.getValue();
            String rel = "acquisition_year=" + part.get(0).year() + "/acquisition_month=" + part.get(0).month()
                    + "/part-00000-" + UUID.randomUUID() + "-c000.snappy.parquet";
            Path file = table.resolve(rel);
            Files.createDirectories(file.getParent());
            writeParquet(file, part);
            String min = part.stream().map(Row::date).min(String::compareTo).orElseThrow();
            String max = part.stream().map(Row::date).max(String::compareTo).orElseThrow();
            long n = declaredRecords != null ? declaredRecords : part.size();
            String stats = "{\"numRecords\":" + n + ",\"minValues\":{\"acquisition_date\":\"" + min
                    + "\"},\"maxValues\":{\"acquisition_date\":\"" + max + "\"}}";
            log.add("{\"add\":{\"path\":\"" + rel + "\",\"partitionValues\":{\"acquisition_year\":\""
                    + part.get(0).year() + "\",\"acquisition_month\":\"" + part.get(0).month()
                    + "\"},\"size\":" + Files.size(file) + ",\"modificationTime\":0,\"dataChange\":true,\"stats\":"
                    + new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(stats) + "}}");
        }
        Path dlog = table.resolve("_delta_log");
        Files.createDirectories(dlog);
        Files.writeString(dlog.resolve(String.format("%020d.json", 0)), String.join("\n", log) + "\n",
                StandardCharsets.UTF_8);
    }

    private static void writeParquet(Path file, List<Row> rows) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE t (schema_version VARCHAR, record_id VARCHAR, source_sha256 VARCHAR, "
                    + "source_record_number BIGINT, source_product VARCHAR, source_url VARCHAR, latitude DOUBLE, "
                    + "longitude DOUBLE, acquisition_date DATE, acquired_at TIMESTAMPTZ, sensor VARCHAR, "
                    + "platform VARCHAR, source_version VARCHAR, confidence_category VARCHAR, "
                    + "confidence_percent SMALLINT, brightness_i4_k DOUBLE, brightness_i5_k DOUBLE, "
                    + "brightness_k DOUBLE, bright_t31_k DOUBLE, frp_mw DOUBLE, scan_km DOUBLE, track_km DOUBLE, "
                    + "daynight VARCHAR, observation_type VARCHAR, h3_r6 BIGINT)");
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO t VALUES ('1', ?, ?, ?, ?, '[redacted]', ?, ?, "
                    + "CAST(? AS DATE), CAST(? AS TIMESTAMPTZ), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                    + "'active_fire_detection', ?)")) {
                for (Row r : rows) {
                    int i = 1;
                    ps.setString(i++, r.recordId());
                    ps.setString(i++, r.sha());
                    ps.setLong(i++, r.recordNo());
                    ps.setString(i++, r.product());
                    ps.setDouble(i++, r.lat());
                    ps.setDouble(i++, r.lon());
                    ps.setString(i++, r.date());
                    ps.setString(i++, r.date() + " " + r.time() + "+00");
                    ps.setString(i++, r.sensor());
                    ps.setString(i++, r.platform());
                    ps.setString(i++, r.version());
                    ps.setString(i++, r.confCategory());
                    if (r.confPercent() == null) ps.setNull(i++, Types.SMALLINT);
                    else ps.setInt(i++, r.confPercent());
                    for (Double d : new Double[]{r.i4(), r.i5(), r.brightnessK(), r.t31(), r.frp(), r.scan(),
                            r.track()}) {
                        if (d == null) ps.setNull(i++, Types.DOUBLE);
                        else ps.setDouble(i++, d);
                    }
                    ps.setString(i++, r.dayNight());
                    ps.setLong(i, r.h3r6());
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            s.execute("COPY t TO '" + file.toString().replace("\\", "/") + "' (FORMAT parquet)");
        }
    }

    /** So para evitar aviso de import nao usado em assinaturas futuras. */
    @SuppressWarnings("unused")
    private static void unused() throws IOException {}
}
