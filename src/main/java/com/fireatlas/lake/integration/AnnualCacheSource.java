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
    /** Lugares acima disto viram partes (ate 8), evitando que uma resolucao grande segure a carga sozinha. */
    public static final long PLACES_ROWS_PER_PART = 300_000;
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

    public Plan plan(int year, boolean includePlaces) throws IOException {
        return plan(year, includePlaces, message -> {}, AnnualCacheExports.Listener.NONE);
    }

    public List<Integer> historicalYears() throws IOException {
        return DeltaTable.load(storage.getObject(), "gold/map_grid/").files().stream()
                .filter(file -> file.resolution() >= 0 && file.resolution() <= 5)
                .map(DeltaTable.DataFile::year).filter(year -> year >= 1 && year < YEAR)
                .distinct().sorted(java.util.Comparator.reverseOrder()).toList();
    }

    static void validateYear(int year) {
        if (year < 1 || year > YEAR) throw new IllegalArgumentException("ano invalido para cache: " + year);
    }

    /**
     * O listener recebe cada exportacao assim que termina; a importacao pode correr em paralelo com o Athena.
     * O Plan so volta depois de reconferir as versoes Delta: so ele autoriza publicar o cache.
     */
    public Plan plan(int year, boolean includePlaces, Consumer<String> progress, AnnualCacheExports.Listener listener) throws IOException {
        validateYear(year);
        if (includePlaces && year != YEAR) throw new IllegalArgumentException("lugares pertencem ao cache de " + YEAR);
        var lake = storage.getObject();
        var grid = DeltaTable.load(lake, "gold/map_grid/");
        var places = includePlaces ? DeltaTable.loadRaw(lake, "gold/h3_places/") : null;
        // Grade: linhas por [resolucao][mes]; o indice 0 de mes nao e usado.
        Long[][] gridRows = new Long[6][13];
        Long[] placeRows = new Long[6];
        for (Long[] months : gridRows) java.util.Arrays.fill(months, 0L);
        java.util.Arrays.fill(placeRows, 0L);
        for (var file : grid.files()) {
            if (file.year() == year && file.resolution() >= 0 && file.resolution() <= 5) {
                if (file.month() < 1 || file.month() > 12) {
                    throw new IOException("map_grid com particao month invalida: " + file.key());
                }
                int res = file.resolution();
                gridRows[res][file.month()] = addRows(gridRows[res][file.month()], file.numRecords());
            }
        }
        if (includePlaces) for (var file : places.files()) {
            String value = file.partitions().get("resolution");
            if (value == null) throw new IOException("h3_places sem particao resolution no manifesto Delta");
            int res = Integer.parseInt(value);
            if (res >= 0 && res <= 5) placeRows[res] = addRows(placeRows[res], file.numRecords());
        }
        // Grade: um UNLOAD por (resolucao, mes). Lugares: um por resolucao, ou ate 8 partes por hash se grande.
        List<AnnualCacheExports.Request> requests = new ArrayList<>();
        for (int res = 0; res <= 5; res++) {
            for (int month = 1; month <= 12; month++) {
                requests.add(AnnualCacheExports.Request.grid(res, month, gridRows[res][month]));
            }
            if (!includePlaces) continue;
            Long placeCount = placeRows[res];
            int parts = placeCount == null ? 0 : (int) Math.min(AnnualCacheExports.Request.MAX_PARTS,
                    (placeCount + PLACES_ROWS_PER_PART - 1) / PLACES_ROWS_PER_PART);
            if (parts < 2) {
                requests.add(AnnualCacheExports.Request.places(res, placeCount));
            } else {
                for (int part = 1; part <= parts; part++) {
                    requests.add(AnnualCacheExports.Request.placesPart(res, part, parts));
                }
            }
        }
        // Maiores primeiro: chegam antes na fila de importacao, que comeca durante a exportacao.
        requests.sort(java.util.Comparator.comparing(AnnualCacheExports.Request::expectedRows,
                java.util.Comparator.nullsFirst(java.util.Comparator.reverseOrder())));
        long placesVersion = includePlaces ? places.version() : -1;
        var partitions = exports.export(year, grid.version(), placesVersion, List.copyOf(requests), progress, listener);
        // Athena nao suporta time travel Delta. Uma publicacao durante o UNLOAD invalida o plano inteiro.
        progress.accept("Conferindo versoes Delta apos exportacao Athena");
        var currentGrid = DeltaTable.load(lake, "gold/map_grid/");
        var currentPlaces = includePlaces ? DeltaTable.loadRaw(lake, "gold/h3_places/") : null;
        if (currentGrid.version() != grid.version() || (includePlaces && currentPlaces.version() != placesVersion)) {
            throw new IOException("snapshot Delta mudou durante a exportacao Athena; tente novamente para a versao atual");
        }
        return new Plan(grid.version(), placesVersion, partitions);
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
            // Duas threads: a leitura do Parquet nao deixa o COPY esperando (eram 10,5% da espera do banco).
            statement.execute("SET threads=2");
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
