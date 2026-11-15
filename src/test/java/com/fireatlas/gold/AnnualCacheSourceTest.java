package com.fireatlas.gold;

import com.fireatlas.lake.integration.AnnualCacheExports;
import com.fireatlas.lake.integration.AnnualCacheSource;
import com.fireatlas.lake.integration.DeltaTable;
import com.fireatlas.lake.integration.S3LakeStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.ObjectProvider;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class AnnualCacheSourceTest {
    @TempDir Path root;

    @Test
    void planProjectsOnlyActive2026SnapshotAndConfirmsEmptyResolutionsBeforeReturning() throws Exception {
        var table = GoldFixtures.table(root, "gold/map_grid/");
        table.append(List.of(GoldFixtures.Row.viirs(6, 1, "2025-12-31", 5, 2),
                GoldFixtures.Row.viirs(6, 1, "2026-01-01", 3, 2)));
        table.overwrite(6, 2026, List.of(GoldFixtures.Row.viirs(6, 2, "2026-11-13", 7, 4)));
        emptyPlaces();
        var projected = projectGrid(6, "exports/grid/part.parquet", 1);
        var storage = mockStorage();
        var exports = mock(AnnualCacheExports.class);
        when(exports.export(eq(1L), eq(0L), anyList(), any())).thenAnswer(invocation -> {
            List<AnnualCacheExports.Request> requests = invocation.getArgument(2);
            assertEquals(14, requests.size());
            assertEquals(1L, requests.stream().filter(r -> r.kind().equals("GRID") && r.resolution() == 6)
                    .findFirst().orElseThrow().expectedRows());
            assertEquals(13, requests.stream().filter(r -> Long.valueOf(0).equals(r.expectedRows())).count());
            return List.of(projected);
        });
        AnnualCacheSource source = source(storage, exports);
        var plan = source.plan();
        assertEquals(List.of(projected), plan.partitions());
        assertThrows(UnsupportedOperationException.class, () -> plan.partitions().clear());
        List<Object[]> rows = new ArrayList<>();
        assertEquals(1, source.stream(projected, rows::addAll));
        assertEquals(2L, rows.getFirst()[2]);
        assertEquals(7L, rows.getFirst()[3]);
        assertEquals(10, rows.getFirst().length);
        verify(storage, never()).download(anyString());
        verify(storage).downloadLocation(projected.key());
    }

    @Test
    void refusesSnapshotPublishedWhileAthenaWasExporting() throws Exception {
        var table = GoldFixtures.table(root, "gold/map_grid/");
        table.append(List.of(GoldFixtures.Row.viirs(6, 1, "2026-01-01", 3, 2)));
        emptyPlaces();
        var exports = mock(AnnualCacheExports.class);
        when(exports.export(anyLong(), anyLong(), anyList(), any())).thenAnswer(invocation -> {
            table.overwrite(6, 2026, List.of(GoldFixtures.Row.viirs(6, 2, "2026-11-13", 7, 4)));
            return List.of();
        });
        var error = assertThrows(java.io.IOException.class, () -> source(mockStorage(), exports).plan());
        assertTrue(error.getMessage().contains("snapshot Delta mudou"));
    }

    @Test
    void streamsBoundedProjectedBatchesAndReleasesFileOnConsumerFailure() throws Exception {
        List<GoldFixtures.Row> rows = new ArrayList<>();
        for (long i = 1; i <= 2501; i++) rows.add(GoldFixtures.Row.viirs(6, i, "2026-11-13", 1, 2));
        GoldFixtures.table(root, "gold/map_grid/").append(rows);
        var projected = projectGrid(6, "exports/grid/part.parquet", 2501);
        var storage = mockStorage();
        AnnualCacheSource source = source(storage, mock(AnnualCacheExports.class));
        List<Integer> sizes = new ArrayList<>();
        assertEquals(2501, source.stream(projected, batch -> sizes.add(batch.size())));
        assertEquals(List.of(1000, 1000, 501), sizes);
        assertThrows(IllegalStateException.class, () -> source.stream(projected, batch -> {
            throw new IllegalStateException("falha de persistencia");
        }));
        verify(storage, times(2)).release(any(Path.class));
        verify(storage, never()).download(anyString());
    }

    @Test
    void independentWorkersReadDifferentProjectedFilesAndCleanDistinctTemporaries() throws Exception {
        List<GoldFixtures.Row> rows = new ArrayList<>();
        for (int res : List.of(5, 6)) {
            for (long cell = 1; cell <= 1001; cell++) rows.add(GoldFixtures.Row.viirs(res, cell, "2026-11-13", 1, 2));
        }
        GoldFixtures.table(root, "gold/map_grid/").append(rows);
        var first = projectGrid(5, "exports/a/part.parquet", 1001);
        var second = projectGrid(6, "exports/b/part.parquet", 1001);
        var storage = mockStorage();
        var downloaded = new ConcurrentLinkedQueue<Path>();
        var local = new GoldFixtures.LocalStorage(root);
        doAnswer(invocation -> {
            String uri = invocation.getArgument(0);
            Path file = local.download(uri.substring("s3://results/".length()));
            downloaded.add(file);
            return file;
        }).when(storage).downloadLocation(anyString());
        AnnualCacheSource source = source(storage, mock(AnnualCacheExports.class));
        var entered = new CountDownLatch(2);
        Consumer<List<Object[]>> consumer = batch -> {
            if (batch.size() == 1000) {
                entered.countDown();
                try {
                    assertTrue(entered.await(10, TimeUnit.SECONDS), "dois streams devem coexistir");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
        };
        try (var executor = Executors.newFixedThreadPool(2)) {
            var a = executor.submit(() -> source.stream(first, consumer));
            var b = executor.submit(() -> source.stream(second, consumer));
            assertEquals(1001L, a.get(15, TimeUnit.SECONDS));
            assertEquals(1001L, b.get(15, TimeUnit.SECONDS));
        }
        assertEquals(2, downloaded.size());
        assertEquals(2, downloaded.stream().map(Path::getParent).distinct().count());
        assertTrue(downloaded.stream().noneMatch(Files::exists));
        verify(storage, times(2)).release(any(Path.class));
    }

    @Test
    void keepsPublishedPlaceCoordinatesAndNeverRecomputesThem() throws Exception {
        Path file = root.resolve("exports/places/part.parquet");
        Files.createDirectories(file.getParent());
        try (var c = DriverManager.getConnection("jdbc:duckdb:"); var statement = c.createStatement()) {
            statement.execute("COPY (SELECT 123::BIGINT AS h3_cell, 'SA' AS continent, 'BRA' AS country_code, "
                    + "'BR' AS country_iso_a2, 'Brazil' AS country_name, 'Brasil' AS country_name_pt, "
                    + "456::BIGINT AS city_geonameid, 'Manaus' AS city_name, 'BR' AS city_country_code, "
                    + "7.25::DOUBLE AS city_distance_km, -3.14::DOUBLE AS latitude, -59.91::DOUBLE AS longitude) "
                    + "TO '" + literal(file) + "' (FORMAT PARQUET, COMPRESSION ZSTD)");
        }
        var partition = new AnnualCacheSource.Partition("s3://results/exports/places/part.parquet", "PLACES", 6, -1, 1L, false);
        List<Object[]> rows = new ArrayList<>();
        assertEquals(1, source(mockStorage(), mock(AnnualCacheExports.class)).stream(partition, rows::addAll));
        assertEquals(12, rows.getFirst().length);
        assertEquals(-3.14, rows.getFirst()[10]);
        assertEquals(-59.91, rows.getFirst()[11]);
    }

    @Test
    void originalParquetKeysAreRejectedRatherThanSilentlyFallingBack() throws Exception {
        var storage = mockStorage();
        var raw = new AnnualCacheSource.Partition("gold/map_grid/part.parquet", "GRID", 6, 100, 1L, false);
        assertThrows(java.io.IOException.class, () -> source(storage, mock(AnnualCacheExports.class)).stream(raw, batch -> {}));
        verify(storage, never()).download(anyString());
        verify(storage, never()).downloadLocation(anyString());
    }

    private AnnualCacheSource.Partition projectGrid(int resolution, String output, long rows) throws Exception {
        var snapshot = DeltaTable.load(new GoldFixtures.LocalStorage(root), "gold/map_grid/");
        var source = snapshot.files().stream().filter(f -> f.year() == 2026 && f.resolution() == resolution)
                .findFirst().orElseThrow();
        Path file = root.resolve(output);
        Files.createDirectories(file.getParent());
        try (var c = DriverManager.getConnection("jdbc:duckdb:"); var statement = c.createStatement()) {
            statement.execute("COPY (SELECT " + String.join(",", AnnualCacheSource.GRID_COLUMNS)
                    + " FROM read_parquet('" + literal(root.resolve(source.key())) + "', hive_partitioning=false)) TO '"
                    + literal(file) + "' (FORMAT PARQUET, COMPRESSION ZSTD)");
        }
        return new AnnualCacheSource.Partition("s3://results/" + output, "GRID", resolution, Files.size(file), rows, false);
    }

    private static String literal(Path path) {
        return path.toAbsolutePath().toString().replace("\\", "/").replace("'", "''");
    }

    private void emptyPlaces() throws Exception {
        Path log = root.resolve("gold/h3_places/_delta_log");
        Files.createDirectories(log);
        Files.writeString(log.resolve("00000000000000000000.json"),
                "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n");
    }

    private S3LakeStorage mockStorage() throws Exception {
        var local = new GoldFixtures.LocalStorage(root);
        var storage = mock(S3LakeStorage.class);
        when(storage.list(anyString())).thenAnswer(invocation -> local.list(invocation.getArgument(0)));
        when(storage.read(anyString())).thenAnswer(invocation -> local.read(invocation.getArgument(0)));
        when(storage.download(anyString())).thenAnswer(invocation -> local.download(invocation.getArgument(0)));
        when(storage.downloadLocation(anyString())).thenAnswer(invocation -> {
            String uri = invocation.getArgument(0);
            return local.download(uri.substring("s3://results/".length()));
        });
        doAnswer(invocation -> { local.release(invocation.getArgument(0)); return null; })
                .when(storage).release(any(Path.class));
        return storage;
    }

    @SuppressWarnings("unchecked")
    private AnnualCacheSource source(S3LakeStorage storage, AnnualCacheExports exports) {
        ObjectProvider<S3LakeStorage> provider = mock(ObjectProvider.class);
        when(provider.getObject()).thenReturn(storage);
        return new AnnualCacheSource(provider, exports);
    }
}
