package com.fireatlas;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Somente para testes: fabrica as tabelas Delta de apoio da gold, no formato real: {@code gold/h3_places}
 * (particionada por resolution) e {@code gold/forecast} (particionada por year e month), com Parquet de verdade e o
 * _delta_log escrito a mao. Faz o papel do pipeline do time de dados.
 */
final class CatalogFixtures {

    private CatalogFixtures() {}

    /** Linha de h3_places; resolution e a particao (a celula tem de ser da mesma resolucao). */
    record PlaceRow(int res, long cell, String continent, String countryCode, String iso2, String name, String namePt,
                    Double share, Long cityId, String cityName, String cityCountry, Double cityKm) {}

    /** Linha de forecast. */
    record ForecastRow(String issueDate, long h3r6, long h3r1, double prob, boolean active, double det24h,
                       double det7d) {}

    static void writePlaces(Path root, List<PlaceRow> rows) throws Exception {
        Map<Integer, List<PlaceRow>> byRes = rows.stream().collect(Collectors.groupingBy(PlaceRow::res, TreeMap::new,
                Collectors.toList()));
        Path table = root.resolve("gold/h3_places");
        List<String> log = header("[\"resolution\"]");
        for (var e : byRes.entrySet()) {
            String rel = "resolution=" + e.getKey() + "/part-00000-" + UUID.randomUUID() + "-c000.snappy.parquet";
            Path file = table.resolve(rel);
            Files.createDirectories(file.getParent());
            try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement s = c.createStatement()) {
                s.execute("CREATE TABLE t (h3_cell BIGINT, latitude DOUBLE, longitude DOUBLE, continent VARCHAR, "
                        + "country_code VARCHAR, country_iso_a2 VARCHAR, country_name VARCHAR, country_name_pt VARCHAR, "
                        + "country_share DOUBLE, country_method VARCHAR, detections BIGINT, city_geonameid BIGINT, "
                        + "city_name VARCHAR, city_country_code VARCHAR, city_distance_km DOUBLE)");
                try (PreparedStatement ps = c.prepareStatement(
                        "INSERT INTO t VALUES (?, 0, 0, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)")) {
                    for (PlaceRow r : e.getValue()) {
                        ps.setLong(1, r.cell());
                        ps.setString(2, r.continent());
                        ps.setString(3, r.countryCode());
                        ps.setString(4, r.iso2());
                        ps.setString(5, r.name());
                        ps.setString(6, r.namePt());
                        if (r.share() == null) ps.setNull(7, Types.DOUBLE);
                        else ps.setDouble(7, r.share());
                        ps.setString(8, r.countryCode() == null ? null : "predominant");
                        if (r.cityId() == null) ps.setNull(9, Types.BIGINT);
                        else ps.setLong(9, r.cityId());
                        ps.setString(10, r.cityName());
                        ps.setString(11, r.cityCountry());
                        if (r.cityKm() == null) ps.setNull(12, Types.DOUBLE);
                        else ps.setDouble(12, r.cityKm());
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }
                s.execute("COPY t TO '" + file.toString().replace("\\", "/") + "' (FORMAT parquet)");
            }
            log.add(add(rel, "{\"resolution\":\"" + e.getKey() + "\"}", file, e.getValue().size(), null));
        }
        commit(table, log);
    }

    static void writeForecast(Path root, List<ForecastRow> rows) throws Exception {
        Path table = root.resolve("gold/forecast");
        List<String> log = header("[\"year\",\"month\"]");
        String rel = "year=2026/month=9/part-00000-" + UUID.randomUUID() + "-c000.snappy.parquet";
        Path file = table.resolve(rel);
        Files.createDirectories(file.getParent());
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE t (issue_date DATE, h3_r6 BIGINT, h3_r1 BIGINT, det_24h DOUBLE, det_3d DOUBLE, "
                    + "det_7d DOUBLE, frp_mean_7d DOUBLE, modis_brightness_mean_7d DOUBLE, "
                    + "viirs_brightness_mean_7d DOUBLE, ring1_det_24h DOUBLE, ring1_det_7d DOUBLE, "
                    + "ring2_det_7d DOUBLE, active BOOLEAN, prob_fire_7d FLOAT)");
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO t VALUES (CAST(? AS DATE), ?, ?, ?, 1, ?, 3.5, NULL, 310.0, 0, 0, 0, ?, ?)")) {
                for (ForecastRow r : rows) {
                    ps.setString(1, r.issueDate());
                    ps.setLong(2, r.h3r6());
                    ps.setLong(3, r.h3r1());
                    ps.setDouble(4, r.det24h());
                    ps.setDouble(5, r.det7d());
                    ps.setBoolean(6, r.active());
                    ps.setFloat(7, (float) r.prob());
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            s.execute("COPY t TO '" + file.toString().replace("\\", "/") + "' (FORMAT parquet)");
        }
        String min = rows.stream().map(ForecastRow::issueDate).min(String::compareTo).orElseThrow();
        String max = rows.stream().map(ForecastRow::issueDate).max(String::compareTo).orElseThrow();
        log.add(add(rel, "{\"year\":\"2026\",\"month\":\"9\"}", file, rows.size(),
                "\"minValues\":{\"issue_date\":\"" + min + "\"},\"maxValues\":{\"issue_date\":\"" + max + "\"}"));
        commit(table, log);
    }


    // ------------------------------------------------------------------------------ atividade harmonizada

    /** Linha de gold/harmonized/cells (resolution e a particao; a celula tem de ser da mesma resolucao). */
    record HarmRow(int res, long cell, String day, double harmonized, int contributing) {}

    /** Linha de gold/harmonized/availability. */
    record AvailRow(String day, String platform, int units, double typical, boolean available, boolean contributes) {}

    private static final String[] SATS = {"terra", "aqua", "snpp", "noaa20", "noaa21"};

    /** Tabela Delta particionada por resolution e year, com os 5 satelites como no pipeline (units em tinyint). */
    static void writeHarmonized(Path root, List<HarmRow> rows) throws Exception {
        Path table = root.resolve("gold/harmonized/cells");
        List<String> log = header("[\"resolution\",\"year\"]");
        Map<String, List<HarmRow>> groups = rows.stream().collect(Collectors.groupingBy(
                r -> r.res() + "|" + r.day().substring(0, 4), TreeMap::new, Collectors.toList()));
        for (var e : groups.entrySet()) {
            List<HarmRow> part = e.getValue();
            int res = part.get(0).res();
            String year = part.get(0).day().substring(0, 4);
            String rel = "resolution=" + res + "/year=" + year + "/part-00000-" + UUID.randomUUID()
                    + "-c000.snappy.parquet";
            Path file = table.resolve(rel);
            Files.createDirectories(file.getParent());
            try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement s = c.createStatement()) {
                StringBuilder ddl = new StringBuilder("CREATE TABLE t (day DATE, h3_cell BIGINT");
                for (String sat : SATS) {
                    ddl.append(", units_").append(sat).append(" TINYINT, detections_").append(sat)
                            .append(" INTEGER, aqua_eq_").append(sat).append(" DOUBLE");
                }
                s.execute(ddl.append(", harmonized DOUBLE, contributing TINYINT)").toString());
                StringBuilder ins = new StringBuilder("INSERT INTO t VALUES ");
                boolean first = true;
                for (HarmRow r : part) {
                    if (!first) ins.append(", ");
                    first = false;
                    ins.append("(DATE '").append(r.day()).append("', ").append(r.cell());
                    for (int i = 0; i < SATS.length; i++) {
                        boolean on = i < r.contributing();
                        ins.append(", ").append(on ? 1 : 0).append(", ").append(on ? 2 : 0).append(", ")
                                .append(on ? r.harmonized() / Math.max(1, r.contributing()) : 0.0);
                    }
                    ins.append(", ").append(r.harmonized()).append(", ").append(r.contributing()).append(")");
                }
                s.execute(ins.toString());
                s.execute("COPY t TO '" + file.toString().replace("\\", "/") + "' (FORMAT parquet)");
            }
            String min = part.stream().map(HarmRow::day).min(String::compareTo).orElseThrow();
            String max = part.stream().map(HarmRow::day).max(String::compareTo).orElseThrow();
            log.add(add(rel, "{\"resolution\":\"" + res + "\",\"year\":\"" + year + "\"}", file, part.size(),
                    "\"minValues\":{\"day\":\"" + min + "\"},\"maxValues\":{\"day\":\"" + max + "\"}"));
        }
        commit(table, log);
    }

    /** Tabela Delta particionada por year. */
    static void writeAvailability(Path root, List<AvailRow> rows) throws Exception {
        Path table = root.resolve("gold/harmonized/availability");
        List<String> log = header("[\"year\"]");
        Map<String, List<AvailRow>> groups = rows.stream().collect(Collectors.groupingBy(
                r -> r.day().substring(0, 4), TreeMap::new, Collectors.toList()));
        for (var e : groups.entrySet()) {
            List<AvailRow> part = e.getValue();
            String year = e.getKey();
            String rel = "year=" + year + "/part-00000-" + UUID.randomUUID() + "-c000.snappy.parquet";
            Path file = table.resolve(rel);
            Files.createDirectories(file.getParent());
            try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement s = c.createStatement()) {
                s.execute("CREATE TABLE t (day DATE, platform VARCHAR, units INTEGER, typical_units DOUBLE, "
                        + "available BOOLEAN, contributes BOOLEAN)");
                StringBuilder ins = new StringBuilder("INSERT INTO t VALUES ");
                boolean first = true;
                for (AvailRow r : part) {
                    if (!first) ins.append(", ");
                    first = false;
                    ins.append("(DATE '").append(r.day()).append("', '").append(r.platform()).append("', ")
                            .append(r.units()).append(", ").append(r.typical()).append(", ").append(r.available())
                            .append(", ").append(r.contributes()).append(")");
                }
                s.execute(ins.toString());
                s.execute("COPY t TO '" + file.toString().replace("\\", "/") + "' (FORMAT parquet)");
            }
            String min = part.stream().map(AvailRow::day).min(String::compareTo).orElseThrow();
            String max = part.stream().map(AvailRow::day).max(String::compareTo).orElseThrow();
            log.add(add(rel, "{\"year\":\"" + year + "\"}", file, part.size(),
                    "\"minValues\":{\"day\":\"" + min + "\"},\"maxValues\":{\"day\":\"" + max + "\"}"));
        }
        commit(table, log);
    }

    private static List<String> header(String partitions) {
        List<String> lines = new ArrayList<>();
        lines.add("{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}");
        lines.add("{\"metaData\":{\"id\":\"" + UUID.randomUUID() + "\",\"format\":{\"provider\":\"parquet\","
                + "\"options\":{}},\"schemaString\":\"{}\",\"partitionColumns\":" + partitions
                + ",\"createdTime\":0,\"configuration\":{}}}");
        return lines;
    }

    private static String add(String rel, String partitionValues, Path file, int rows, String extraStats)
            throws IOException {
        String stats = "{\"numRecords\":" + rows + (extraStats == null ? "" : "," + extraStats) + "}";
        return "{\"add\":{\"path\":\"" + rel + "\",\"partitionValues\":" + partitionValues + ",\"size\":"
                + Files.size(file) + ",\"modificationTime\":0,\"dataChange\":true,\"stats\":"
                + new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(stats) + "}}";
    }

    private static void commit(Path table, List<String> lines) throws IOException {
        Path log = table.resolve("_delta_log");
        Files.createDirectories(log);
        Files.writeString(log.resolve(String.format("%020d.json", 0)), String.join("\n", lines) + "\n",
                StandardCharsets.UTF_8);
    }

    /** So para evitar um import nao usado em SQLException em assinaturas futuras. */
    @SuppressWarnings("unused")
    private static void unused() throws SQLException {}
}
