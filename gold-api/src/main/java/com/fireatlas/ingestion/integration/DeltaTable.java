package com.fireatlas.ingestion.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Leitura (somente leitura) de uma tabela Delta Lake: reconstrui o conjunto de arquivos ATIVOS a partir do ultimo
 * checkpoint e dos commits seguintes do {@code _delta_log}. Ler os .parquet soltos pegaria arquivos removidos ou
 * antigos; por isso so os arquivos que o log declara ativos entram na importacao.
 * <p>
 * Suporta o protocolo de leitura 1 (sem deletion vectors nem column mapping), qualquer conjunto de colunas de
 * particao (valores no log, nao no arquivo) e checkpoints em um ou varios arquivos.
 */
public final class DeltaTable {

    /** Arquivo ativo de qualquer tabela: chave, valores de particao, tamanho e estatisticas (min/max por coluna). */
    public record RawFile(String key, Map<String, String> partitions, long size, Long numRecords,
                          Map<String, String> minValues, Map<String, String> maxValues) {

        public LocalDate minDate(String column) {
            return date(minValues.get(column));
        }

        public LocalDate maxDate(String column) {
            return date(maxValues.get(column));
        }
    }

    /** Estado de uma tabela qualquer: versao do log e arquivos ativos. */
    public record RawSnapshot(long version, List<RawFile> files) {}

    /** Arquivo ativo da grade (particionada por resolution e year; estatistica da coluna day). */
    public record DataFile(String key, int resolution, int year, long size, Long numRecords,
                           LocalDate minDay, LocalDate maxDay) {}

    /** Estado da grade: versao do log e arquivos ativos. */
    public record Snapshot(long version, List<DataFile> files) {}

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private DeltaTable() {}

    /** Tabela da grade (map_grid): exige as particoes resolution e year. */
    public static Snapshot load(LakeStorage storage, String tablePath) throws IOException {
        RawSnapshot raw = loadRaw(storage, tablePath);
        List<DataFile> files = new ArrayList<>();
        for (RawFile f : raw.files()) {
            String resolution = f.partitions().get("resolution");
            String year = f.partitions().get("year");
            if (resolution == null || year == null) {
                throw new IOException("arquivo sem particao resolution/year no log: " + f.key());
            }
            files.add(new DataFile(f.key(), Integer.parseInt(resolution), Integer.parseInt(year), f.size(),
                    f.numRecords(), f.minDate("day"), f.maxDate("day")));
        }
        return new Snapshot(raw.version(), List.copyOf(files));
    }

    /** @param tablePath prefixo da tabela, terminado em "/" (ex.: "gold/h3_places/") */
    public static RawSnapshot loadRaw(LakeStorage storage, String tablePath) throws IOException {
        String prefix = tablePath.endsWith("/") ? tablePath : tablePath + "/";
        String logPrefix = prefix + "_delta_log/";
        List<String> keys = storage.list(logPrefix);
        if (keys.isEmpty()) throw new IOException("nao ha _delta_log em " + prefix + ": nao e uma tabela Delta");

        TreeMap<Long, String> commits = new TreeMap<>();
        Map<Long, List<String>> checkpoints = new TreeMap<>();
        for (String key : keys) {
            String name = key.substring(logPrefix.length());
            if (name.contains("/")) continue; // _sidecars etc.
            if (name.matches("\\d{20}\\.json")) {
                commits.put(Long.parseLong(name.substring(0, 20)), key);
            } else if (name.matches("\\d{20}\\.checkpoint(\\.\\d{10}\\.\\d{10})?\\.parquet")) {
                checkpoints.computeIfAbsent(Long.parseLong(name.substring(0, 20)), v -> new ArrayList<>()).add(key);
            }
        }

        // checkpoint mais recente que tenha todas as partes
        long checkpointVersion = -1;
        for (var e : checkpoints.entrySet()) {
            if (completeCheckpoint(e.getValue())) checkpointVersion = Math.max(checkpointVersion, e.getKey());
        }

        Map<String, RawFile> active = new LinkedHashMap<>();
        if (checkpointVersion >= 0) {
            for (String key : checkpoints.get(checkpointVersion)) readCheckpoint(storage, key, prefix, active);
        }

        long expected = checkpointVersion + 1;
        long last = checkpointVersion;
        for (var e : commits.tailMap(checkpointVersion + 1).entrySet()) {
            if (e.getKey() != expected) {
                throw new IOException("log Delta incompleto: faltam commits entre as versoes " + expected + " e "
                        + (e.getKey() - 1));
            }
            applyCommit(new String(storage.read(e.getValue()), StandardCharsets.UTF_8), prefix, active);
            expected++;
            last = e.getKey();
        }
        if (last < 0) throw new IOException("log Delta vazio em " + prefix);
        return new RawSnapshot(last, List.copyOf(active.values()));
    }

    private static boolean completeCheckpoint(List<String> parts) {
        if (parts.size() == 1 && parts.get(0).matches(".*\\d{20}\\.checkpoint\\.parquet")) return true;
        Set<String> seen = new HashSet<>();
        int total = -1;
        for (String p : parts) {
            String[] tokens = p.substring(p.lastIndexOf('/') + 1).split("\\.");
            if (tokens.length == 5) {
                total = Integer.parseInt(tokens[3]);
                seen.add(tokens[2]);
            }
        }
        return total > 0 && seen.size() == total;
    }

    private static void applyCommit(String ndjson, String prefix, Map<String, RawFile> active) throws IOException {
        for (String line : ndjson.split("\n")) {
            if (line.isBlank()) continue;
            JsonNode node = MAPPER.readTree(line);
            if (node.has("protocol")) checkProtocol(node.get("protocol").path("minReaderVersion").asInt(1),
                    node.get("protocol").has("readerFeatures") && node.get("protocol").get("readerFeatures").size() > 0);
            if (node.has("add")) {
                JsonNode add = node.get("add");
                Map<String, String> pv = new LinkedHashMap<>();
                add.path("partitionValues").fields().forEachRemaining(f -> pv.put(f.getKey(), f.getValue().asText()));
                put(active, prefix, add.path("path").asText(), pv, add.path("size").asLong(),
                        add.path("stats").isMissingNode() || add.path("stats").isNull() ? null
                                : add.path("stats").asText());
            } else if (node.has("remove")) {
                active.remove(decode(prefix, node.get("remove").path("path").asText()));
            }
        }
    }

    private static void readCheckpoint(LakeStorage storage, String key, String prefix,
                                       Map<String, RawFile> active) throws IOException {
        Path file = storage.download(key);
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement s = c.createStatement()) {
            String lit = "'" + file.toAbsolutePath().toString().replace("\\", "/").replace("'", "''") + "'";
            Set<String> cols = new HashSet<>();
            try (ResultSet rs = s.executeQuery("DESCRIBE SELECT * FROM read_parquet(" + lit + ")")) {
                while (rs.next()) cols.add(rs.getString("column_name"));
            }
            if (!cols.contains("add")) throw new IOException("checkpoint sem coluna add: " + key);
            if (cols.contains("sidecar")) {
                try (ResultSet rs = s.executeQuery("SELECT count(*) FROM read_parquet(" + lit
                        + ") WHERE sidecar IS NOT NULL")) {
                    if (rs.next() && rs.getLong(1) > 0) {
                        throw new IOException("checkpoint com sidecars (v2) nao suportado: " + key);
                    }
                }
            }
            if (cols.contains("protocol")) {
                try (ResultSet rs = s.executeQuery("SELECT protocol.minReaderVersion, "
                        + "coalesce(len(protocol.readerFeatures), 0) FROM read_parquet(" + lit
                        + ") WHERE protocol IS NOT NULL")) {
                    while (rs.next()) checkProtocol(rs.getInt(1), rs.getInt(2) > 0);
                }
            }
            // partitionValues e um MAP: sai como texto "{chave=valor, chave=valor}"
            try (ResultSet rs = s.executeQuery("SELECT \"add\".path, CAST(\"add\".partitionValues AS VARCHAR), "
                    + "\"add\".size, \"add\".stats FROM read_parquet(" + lit + ") WHERE \"add\" IS NOT NULL")) {
                while (rs.next()) {
                    put(active, prefix, rs.getString(1), parseMap(rs.getString(2)), rs.getLong(3), rs.getString(4));
                }
            }
        } catch (SQLException e) {
            throw new IOException("checkpoint do Delta ilegivel (" + key + "): " + e.getMessage(), e);
        } finally {
            storage.release(file);
        }
    }

    /** "{year=2014, resolution=6}" -> mapa. */
    static Map<String, String> parseMap(String text) {
        Map<String, String> out = new LinkedHashMap<>();
        if (text == null) return out;
        String body = text.trim();
        if (body.startsWith("{")) body = body.substring(1);
        if (body.endsWith("}")) body = body.substring(0, body.length() - 1);
        if (body.isBlank()) return out;
        for (String pair : body.split(",\\s*")) {
            int eq = pair.indexOf('=');
            if (eq > 0) out.put(pair.substring(0, eq).trim(), pair.substring(eq + 1).trim());
        }
        return out;
    }

    private static void checkProtocol(int minReaderVersion, boolean hasReaderFeatures) throws IOException {
        if (minReaderVersion > 1 || hasReaderFeatures) {
            throw new IOException("tabela Delta exige recursos de leitura nao suportados (minReaderVersion="
                    + minReaderVersion + ")");
        }
    }

    private static void put(Map<String, RawFile> active, String prefix, String path, Map<String, String> pv,
                            long size, String stats) throws IOException {
        Long records = null;
        Map<String, String> min = new LinkedHashMap<>();
        Map<String, String> max = new LinkedHashMap<>();
        if (stats != null && !stats.isBlank()) {
            JsonNode st = MAPPER.readTree(stats);
            if (st.hasNonNull("numRecords")) records = st.get("numRecords").asLong();
            st.path("minValues").fields().forEachRemaining(f -> {
                if (f.getValue().isValueNode()) min.put(f.getKey(), f.getValue().asText());
            });
            st.path("maxValues").fields().forEachRemaining(f -> {
                if (f.getValue().isValueNode()) max.put(f.getKey(), f.getValue().asText());
            });
        }
        String key = decode(prefix, path);
        active.put(key, new RawFile(key, pv, size, records, min, max));
    }

    private static LocalDate date(String text) {
        if (text == null || text.length() < 10) return null;
        try {
            return LocalDate.parse(text.substring(0, 10));
        } catch (RuntimeException e) {
            return null;
        }
    }

    /** Caminhos do log sao relativos a tabela e podem vir codificados como URL. */
    private static String decode(String prefix, String path) {
        return prefix + java.net.URLDecoder.decode(path.replace("+", "%2B"), StandardCharsets.UTF_8);
    }
}
