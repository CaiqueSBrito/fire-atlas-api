package com.fireatlas;

import com.fireatlas.ingestion.integration.LakeStorage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Somente para testes: fabrica uma tabela Delta no formato da gold (grade H3 por dia e sensor, particionada por
 * resolution e year) em uma pasta local, com Parquet de verdade (DuckDB) e o _delta_log escrito a mao. Faz o papel
 * do pipeline que gera a gold: o backend so le.
 */
final class GoldFixtures {

    private GoldFixtures() {}

    /** Uma linha da grade; res e a particao (a celula tem de ser da mesma resolucao). */
    record Row(int res, long cell, String day, String sensor, long detections, double frpSum, double frpMax,
               long confLow, long confNominal, long confHigh, Double modisConfMean) {

        static Row viirs(int res, long cell, String day, long n, double frpEach) {
            return new Row(res, cell, day, "VIIRS", n, n * frpEach, frpEach + 1, 0, n, 0, null);
        }

        static Row modis(int res, long cell, String day, long n, double frpEach) {
            return new Row(res, cell, day, "MODIS", n, n * frpEach, frpEach + 1, 0, 0, 0, 80.0);
        }

        int year() {
            return Integer.parseInt(day.substring(0, 4));
        }
    }

    /** As linhas de um foco em TODAS as resolucoes de 0 a 6, como a gold traz (cada resolucao ja agregada). */
    static List<Row> allResolutions(double lat, double lon, String day, String sensor, long n, double frpEach) {
        return IntStream.rangeClosed(0, 6).mapToObj(res -> "MODIS".equals(sensor)
                ? Row.modis(res, TestH3.cell(lat, lon, res), day, n, frpEach)
                : Row.viirs(res, TestH3.cell(lat, lon, res), day, n, frpEach)).toList();
    }

    /** Tabela Delta local; cada operacao grava uma nova versao no _delta_log. */
    static final class Table {
        final Path root;
        final String prefix;
        private int version = -1;
        private final Map<String, String> activeAdds = new TreeMap<>(); // path -> linha add (json)

        Table(Path root, String prefix) {
            this.root = root;
            this.prefix = prefix.endsWith("/") ? prefix : prefix + "/";
        }

        Path tableDir() {
            return root.resolve(prefix);
        }

        /** Nova versao: adiciona as linhas (um arquivo por resolucao e ano). */
        Table append(List<Row> rows) throws Exception {
            List<String> lines = new ArrayList<>();
            if (version < 0) {
                lines.add("{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}");
                lines.add("{\"metaData\":{\"id\":\"" + UUID.randomUUID() + "\",\"format\":{\"provider\":\"parquet\","
                        + "\"options\":{}},\"schemaString\":\"{}\",\"partitionColumns\":[\"resolution\",\"year\"],"
                        + "\"createdTime\":0,\"configuration\":{}}}");
            }
            lines.addAll(writeFiles(rows));
            commit(lines);
            return this;
        }

        /** Nova versao que substitui a particao (resolucao, ano): remove o(s) arquivo(s) antigo(s) e adiciona o novo. */
        Table overwrite(int res, int year, List<Row> rows) throws Exception {
            List<String> lines = new ArrayList<>();
            String part = "resolution=" + res + "/year=" + year + "/";
            for (String path : new ArrayList<>(activeAdds.keySet())) {
                if (path.startsWith(part)) {
                    lines.add("{\"remove\":{\"path\":\"" + path + "\",\"deletionTimestamp\":1,\"dataChange\":true}}");
                    activeAdds.remove(path);
                }
            }
            lines.addAll(writeFiles(rows));
            commit(lines);
            return this;
        }

        /** Escreve um checkpoint (Parquet) do estado atual e o _last_checkpoint, como o Delta faz a cada N commits. */
        Table checkpoint() throws Exception {
            Path log = tableDir().resolve("_delta_log");
            String name = String.format("%020d.checkpoint.parquet", version);
            try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement s = c.createStatement()) {
                s.execute("CREATE TABLE ck (path VARCHAR, res VARCHAR, yr VARCHAR, size BIGINT, stats VARCHAR)");
                try (PreparedStatement ps = c.prepareStatement("INSERT INTO ck VALUES (?, ?, ?, ?, ?)")) {
                    for (String line : activeAdds.values()) {
                        var node = new com.fasterxml.jackson.databind.ObjectMapper().readTree(line).get("add");
                        ps.setString(1, node.get("path").asText());
                        ps.setString(2, node.get("partitionValues").get("resolution").asText());
                        ps.setString(3, node.get("partitionValues").get("year").asText());
                        ps.setLong(4, node.get("size").asLong());
                        ps.setString(5, node.get("stats").asText());
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }
                s.execute("COPY (SELECT struct_pack(path := path, partitionValues := map(['resolution', 'year'], "
                        + "[res, yr]), size := size, stats := stats) AS \"add\" FROM ck) TO '"
                        + log.resolve(name).toString().replace("\\", "/") + "' (FORMAT parquet)");
            }
            Files.writeString(log.resolve("_last_checkpoint"), "{\"version\":" + version + ",\"size\":1}",
                    StandardCharsets.UTF_8);
            return this;
        }

        private void commit(List<String> lines) throws IOException {
            version++;
            Path log = tableDir().resolve("_delta_log");
            Files.createDirectories(log);
            Files.writeString(log.resolve(String.format("%020d.json", version)),
                    String.join("\n", lines) + "\n", StandardCharsets.UTF_8);
        }

        private List<String> writeFiles(List<Row> input) throws Exception {
            List<Row> rows = aggregate(input);
            List<String> lines = new ArrayList<>();
            Map<String, List<Row>> groups = rows.stream().collect(Collectors.groupingBy(
                    r -> r.res() + "/" + r.year(), TreeMap::new, Collectors.toList()));
            for (var g : groups.entrySet()) {
                List<Row> part = g.getValue();
                int res = part.get(0).res();
                int year = part.get(0).year();
                String rel = "resolution=" + res + "/year=" + year + "/part-00000-" + UUID.randomUUID()
                        + "-c000.zstd.parquet";
                Path file = tableDir().resolve(rel);
                Files.createDirectories(file.getParent());
                writeParquet(file, part);
                String min = part.stream().map(Row::day).min(Comparator.naturalOrder()).orElseThrow();
                String max = part.stream().map(Row::day).max(Comparator.naturalOrder()).orElseThrow();
                String stats = "{\"numRecords\":" + part.size() + ",\"minValues\":{\"day\":\"" + min
                        + "\"},\"maxValues\":{\"day\":\"" + max + "\"}}";
                String add = "{\"add\":{\"path\":\"" + rel + "\",\"partitionValues\":{\"year\":\"" + year
                        + "\",\"resolution\":\"" + res + "\"},\"size\":" + Files.size(file)
                        + ",\"modificationTime\":0,\"dataChange\":true,\"stats\":"
                        + new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(stats) + "}}";
                activeAdds.put(rel, add);
                lines.add(add);
            }
            return lines;
        }

        /** Estraga o conteudo de todos os arquivos de dados (o log continua valido): simula arquivo corrompido. */
        Table corruptDataFiles() throws IOException {
            try (Stream<Path> files = Files.walk(tableDir())) {
                for (Path f : files.filter(p -> p.toString().endsWith(".parquet")
                        && !p.toString().contains("_delta_log")).toList()) {
                    Files.writeString(f, "isto nao e um parquet");
                }
            }
            return this;
        }
    }

    /** A gold traz uma linha por (resolucao, celula, dia, sensor): soma as linhas que caem na mesma chave. */
    static List<Row> aggregate(List<Row> rows) {
        Map<String, Row> merged = new java.util.LinkedHashMap<>();
        for (Row r : rows) {
            String key = r.res() + "|" + r.cell() + "|" + r.day() + "|" + r.sensor();
            merged.merge(key, r, (a, b) -> new Row(a.res(), a.cell(), a.day(), a.sensor(),
                    a.detections() + b.detections(), a.frpSum() + b.frpSum(), Math.max(a.frpMax(), b.frpMax()),
                    a.confLow() + b.confLow(), a.confNominal() + b.confNominal(), a.confHigh() + b.confHigh(),
                    mergeConf(a, b)));
        }
        return new ArrayList<>(merged.values());
    }

    private static Double mergeConf(Row a, Row b) {
        if (a.modisConfMean() == null) return b.modisConfMean();
        if (b.modisConfMean() == null) return a.modisConfMean();
        return (a.modisConfMean() * a.detections() + b.modisConfMean() * b.detections())
                / (a.detections() + b.detections());
    }

    static Table table(Path root, String prefix) {
        return new Table(root, prefix);
    }

    private static void writeParquet(Path file, List<Row> rows) throws SQLException {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE t (h3_cell BIGINT, day DATE, sensor VARCHAR, detections BIGINT, platforms INTEGER, "
                    + "frp_sum_mw DOUBLE, frp_mean_mw DOUBLE, frp_max_mw DOUBLE, brightness_max_k DOUBLE, "
                    + "viirs_confidence_low BIGINT, viirs_confidence_nominal BIGINT, viirs_confidence_high BIGINT, "
                    + "modis_confidence_mean DOUBLE)");
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO t VALUES (?, CAST(? AS DATE), ?, ?, 1, ?, ?, ?, 330.5, ?, ?, ?, ?)")) {
                for (Row r : rows) {
                    ps.setLong(1, r.cell());
                    ps.setString(2, r.day());
                    ps.setString(3, r.sensor());
                    ps.setLong(4, r.detections());
                    ps.setDouble(5, r.frpSum());
                    ps.setDouble(6, r.detections() == 0 ? 0 : r.frpSum() / r.detections());
                    ps.setDouble(7, r.frpMax());
                    ps.setLong(8, r.confLow());
                    ps.setLong(9, r.confNominal());
                    ps.setLong(10, r.confHigh());
                    if (r.modisConfMean() == null) ps.setNull(11, java.sql.Types.DOUBLE);
                    else ps.setDouble(11, r.modisConfMean());
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            s.execute("COPY t TO '" + file.toString().replace("\\", "/") + "' (FORMAT parquet, COMPRESSION zstd)");
        }
    }

    /** Armazenamento do data lake em uma pasta local; permite simular falha de leitura em chaves escolhidas. */
    static final class LocalStorage implements LakeStorage {
        private final Path root;
        private Predicate<String> failOn = k -> false;

        LocalStorage(Path root) {
            this.root = root;
        }

        LocalStorage failingOn(Predicate<String> predicate) {
            this.failOn = predicate;
            return this;
        }

        @Override
        public List<String> list(String prefix) throws IOException {
            if (failOn.test(prefix)) throw new IOException("falha simulada ao listar " + prefix);
            try (Stream<Path> files = Files.walk(root)) {
                return files.filter(Files::isRegularFile)
                        .map(p -> root.relativize(p).toString().replace("\\", "/"))
                        .filter(k -> k.startsWith(prefix)).sorted().toList();
            }
        }

        @Override
        public byte[] read(String key) throws IOException {
            if (failOn.test(key)) throw new IOException("falha simulada ao ler " + key);
            return Files.readAllBytes(root.resolve(key));
        }

        @Override
        public Path download(String key) throws IOException {
            if (failOn.test(key)) throw new IOException("falha simulada ao baixar " + key);
            Path dir = Files.createTempDirectory("fireatlas-test-lake-");
            Path copy = dir.resolve(key.substring(key.lastIndexOf('/') + 1));
            Files.copy(root.resolve(key), copy);
            return copy;
        }

        @Override
        public void release(Path file) {
            try {
                Files.deleteIfExists(file);
                Files.deleteIfExists(file.getParent());
            } catch (IOException ignored) {
                // temporario de teste
            }
        }
    }
}
