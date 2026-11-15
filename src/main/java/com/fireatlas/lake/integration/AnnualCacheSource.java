package com.fireatlas.lake.integration;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

/** Snapshot Delta verificado antes/depois do UNLOAD; baixa somente Parquet projetado no Athena. */
@Component
public class AnnualCacheSource {
    public static final int YEAR = 2026;
    public static final int BATCH_SIZE = 1000;
    public static final List<String> GRID_COLUMNS = List.of("day", "sensor", "h3_cell", "detections",
            "frp_sum_mw", "frp_max_mw", "viirs_confidence_low", "viirs_confidence_nominal",
            "viirs_confidence_high", "modis_confidence_mean");
    public static final List<String> PLACE_COLUMNS = List.of("h3_cell", "continent", "country_code", "country_iso_a2",
            "country_name", "country_name_pt", "city_geonameid", "city_name", "city_country_code", "city_distance_km",
            "latitude", "longitude");

    public record Partition(String key, String kind, int resolution, long size, Long expectedRows, boolean empty) {}
    public record Plan(long gridVersion, long placesVersion, List<Partition> partitions) {
        public Plan { partitions = List.copyOf(partitions); }
    }

    private final ObjectProvider<S3LakeStorage> storage;
    private final AnnualCacheExports exports;

    public AnnualCacheSource(ObjectProvider<S3LakeStorage> storage, AnnualCacheExports exports) {
        this.storage = storage;
        this.exports = exports;
    }

    public Plan plan() throws IOException {
        return plan(message -> {});
    }

    public Plan plan(Consumer<String> progress) throws IOException {
        var lake = storage.getObject();
        var grid = DeltaTable.load(lake, "gold/map_grid/");
        var places = DeltaTable.loadRaw(lake, "gold/h3_places/");
        Long[] gridRows = new Long[7];
        Long[] placeRows = new Long[7];
        java.util.Arrays.fill(gridRows, 0L);
        java.util.Arrays.fill(placeRows, 0L);
        for (var file : grid.files()) {
            if (file.year() == YEAR && file.resolution() >= 0 && file.resolution() <= 6) {
                int res = file.resolution();
                gridRows[res] = addRows(gridRows[res], file.numRecords());
            }
        }
        for (var file : places.files()) {
            String value = file.partitions().get("resolution");
            if (value == null) throw new IOException("h3_places sem particao resolution no manifesto Delta");
            int res = Integer.parseInt(value);
            if (res >= 0 && res <= 6) placeRows[res] = addRows(placeRows[res], file.numRecords());
        }
        List<AnnualCacheExports.Request> requests = new ArrayList<>();
        for (int res = 0; res <= 6; res++) {
            requests.add(new AnnualCacheExports.Request("GRID", res, gridRows[res]));
            requests.add(new AnnualCacheExports.Request("PLACES", res, placeRows[res]));
        }
        var partitions = exports.export(grid.version(), places.version(), List.copyOf(requests), progress);
        // Athena nao suporta time travel Delta. Uma publicacao durante o UNLOAD invalida o plano inteiro.
        progress.accept("Conferindo versoes Delta apos exportacao Athena; publicacao PostGIS ainda pendente");
        var currentGrid = DeltaTable.load(lake, "gold/map_grid/");
        var currentPlaces = DeltaTable.loadRaw(lake, "gold/h3_places/");
        if (currentGrid.version() != grid.version() || currentPlaces.version() != places.version()) {
            throw new IOException("snapshot Delta mudou durante a exportacao Athena; tente novamente para a versao atual");
        }
        return new Plan(grid.version(), places.version(), partitions);
    }

    private static Long addRows(Long total, Long records) {
        return total == null || records == null ? null : Math.addExact(total, records);
    }

    /** Cada worker possui sua conexao DuckDB e seu temporario; nenhum download original e permitido. */
    public long stream(Partition partition, Consumer<List<Object[]>> consumer) throws IOException, SQLException {
        if (partition.empty()) return 0;
        var lake = storage.getObject();
        if (!partition.key().startsWith("s3://")) throw new IOException("particao nao e um objeto exportado pelo Athena");
        Path file = lake.downloadLocation(partition.key());
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             Statement statement = connection.createStatement()) {
            statement.execute("SET memory_limit='128MB'");
            statement.execute("SET threads=1");
            String parquet = "read_parquet('" + file.toAbsolutePath().toString().replace("\\", "/")
                    .replace("'", "''") + "', hive_partitioning=false)";
            Set<String> available = new HashSet<>();
            try (ResultSet columns = statement.executeQuery("DESCRIBE SELECT * FROM " + parquet)) {
                while (columns.next()) available.add(columns.getString("column_name"));
            }
            boolean grid = partition.kind().equals("GRID");
            List<String> columns = grid ? GRID_COLUMNS : PLACE_COLUMNS;
            if (!available.containsAll(columns)) throw new IOException("Parquet projetado sem colunas contratadas");
            String selection = columns.stream().map(c -> "\"" + c + "\"")
                    .collect(java.util.stream.Collectors.joining(", "));
            long count = 0;
            List<Object[]> batch = new ArrayList<>(BATCH_SIZE);
            try (ResultSet rows = statement.executeQuery("SELECT " + selection + " FROM " + parquet)) {
                while (rows.next()) {
                    if (Thread.currentThread().isInterrupted()) throw new IOException("carga interrompida");
                    Object[] values = new Object[columns.size()];
                    for (int i = 0; i < columns.size(); i++) {
                        values[i] = columns.get(i).equals("day") ? rows.getDate(i + 1) : rows.getObject(i + 1);
                    }
                    batch.add(values);
                    count++;
                    if (batch.size() == BATCH_SIZE) {
                        consumer.accept(batch);
                        batch.clear();
                    }
                }
            }
            if (!batch.isEmpty()) consumer.accept(batch);
            if (partition.expectedRows() != null && count != partition.expectedRows()) {
                throw new IOException("numRecords do snapshot divergente: esperado " + partition.expectedRows() + ", lido " + count);
            }
            return count;
        } finally {
            lake.release(file);
        }
    }
}
